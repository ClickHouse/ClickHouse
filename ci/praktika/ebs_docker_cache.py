"""
EBS Snapshot Docker Cache.

Pre-populates Docker's image store from EBS snapshots so that CI runners
do not need to pull images from the registry.  Docker build jobs create
snapshots after building; runner jobs mount them before executing.

Snapshots are tagged with a combined digest (derived from all per-image
digests) and architecture so that runners can look them up with a single
EC2 describe-snapshots call.
"""

import hashlib
import os
import signal
import time
import traceback
from typing import Dict, List, Optional, Tuple

from .settings import Settings
from .utils import Shell, Utils

# --- EBS snapshot tag keys ---
TAG_NAME = "Name"
TAG_NAME_VALUE = "praktika-docker-cache"
TAG_DOCKER_CACHE_DIGEST = "docker-cache-digest"
TAG_ARCH = "arch"

# Mount points and device helpers
_CACHE_MOUNT = "/mnt/docker-cache"
_DOCKER_MOUNT = "/var/lib/docker"
_CACHE_DOCKERD_SOCK = "/var/run/docker-cache.sock"
_CACHE_DOCKERD_PID = "/var/run/docker-cache.pid"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_arch() -> str:
    if Utils.is_arm():
        return "arm"
    elif Utils.is_amd():
        return "amd"
    raise RuntimeError("Unsupported CPU architecture for EBS docker cache")


def _get_instance_metadata() -> Tuple[str, str, str]:
    """Return (instance_id, availability_zone, region)."""
    instance_id = (
        os.environ.get("INSTANCE_ID")
        or Shell.get_output("ec2metadata --instance-id")
        or ""
    )
    az = Shell.get_output(
        "ec2metadata --availability-zone"
    ) or Shell.get_output(
        "curl -s --fail http://169.254.169.254/latest/meta-data/placement/availability-zone"
    )
    region = Settings.AWS_REGION or (az[:-1] if az else "us-east-1")
    if not instance_id or not az:
        raise RuntimeError(
            f"Cannot determine instance metadata: instance_id=[{instance_id}], az=[{az}]"
        )
    return instance_id, az, region


def _ec2_client(region: str):
    import boto3

    return boto3.client("ec2", region_name=region)


def _find_free_device() -> str:
    """Return the first free /dev/xvd[f-z] device name."""
    import string

    for letter in string.ascii_lowercase[5:]:  # f..z
        dev = f"/dev/xvd{letter}"
        if not os.path.exists(dev):
            return dev
    raise RuntimeError("No free /dev/xvd* device found")


def _resolve_device(requested_device: str) -> str:
    """After attaching an EBS volume as e.g. /dev/xvdf, the kernel may
    expose it as /dev/nvme*n1. Wait up to 30 s and return the actual path."""
    for _ in range(30):
        if os.path.exists(requested_device):
            return requested_device
        # On Nitro instances the device appears as /dev/nvme*n1 but we can
        # find it via lsblk or by scanning /dev/disk/by-id/.
        # Simple approach: look for a new block device that appeared.
        time.sleep(1)
    # Last resort: try lsblk
    out = Shell.get_output("lsblk -dpno NAME,SERIAL 2>/dev/null") or ""
    for line in out.strip().splitlines():
        parts = line.split()
        if len(parts) >= 2 and requested_device.replace("/dev/", "").replace(
            "xvd", "vol"
        ) in parts[1].replace("-", ""):
            return parts[0]
    raise RuntimeError(
        f"Device {requested_device} did not appear within 30 seconds"
    )


def _wait_for_volume_status(ec2, volume_id: str, status: str, timeout: int = 120):
    for _ in range(timeout):
        resp = ec2.describe_volumes(VolumeIds=[volume_id])
        vol = resp["Volumes"][0]
        if vol["State"] == status:
            return
        time.sleep(1)
    raise RuntimeError(
        f"Volume {volume_id} did not reach state {status} within {timeout}s"
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def calc_combined_digest(digest_dockers: Dict[str, str]) -> str:
    """Combine per-image digests into a single cache key.

    Uses sorted image names for determinism.
    """
    h = hashlib.sha256()
    for name in sorted(digest_dockers.keys()):
        h.update(f"{name}={digest_dockers[name]}".encode())
    return h.hexdigest()[: Settings.CACHE_DIGEST_LEN]


def find_snapshot(
    combined_digest: str, arch: str, region: str
) -> Optional[str]:
    """Look up a completed EBS snapshot by digest and arch tags.

    Returns snapshot_id if found, None otherwise.
    """
    ec2 = _ec2_client(region)
    resp = ec2.describe_snapshots(
        Filters=[
            {"Name": f"tag:{TAG_DOCKER_CACHE_DIGEST}", "Values": [combined_digest]},
            {"Name": f"tag:{TAG_ARCH}", "Values": [arch]},
            {"Name": "status", "Values": ["completed"]},
        ],
        OwnerIds=["self"],
    )
    snapshots = resp.get("Snapshots", [])
    if snapshots:
        snapshots.sort(key=lambda s: s.get("StartTime", ""), reverse=True)
        snap_id = snapshots[0]["SnapshotId"]
        print(f"EBS docker cache: found snapshot [{snap_id}] for digest=[{combined_digest}] arch=[{arch}]")
        return snap_id
    return None


def create_cache_snapshot(
    docker_configs,
    digests: Dict[str, str],
    combined_digest: str,
    arch: str,
) -> Optional[str]:
    """Create an EBS snapshot containing all Docker images pre-pulled.

    1. Create a gp3 EBS volume in the same AZ as this instance
    2. Attach, format (ext4), mount
    3. Start a secondary dockerd with --data-root on the volume
    4. Pull all images (arch-specific tags)
    5. Stop dockerd, unmount, detach
    6. Create snapshot with tags
    7. Delete the temporary volume

    Returns snapshot_id on success, None on failure.
    """
    instance_id, az, region = _get_instance_metadata()
    ec2 = _ec2_client(region)

    volume_id = None
    device = None
    mounted = False
    dockerd_pid = None

    try:
        # 1. Create volume
        print(f"EBS docker cache: creating {Settings.EBS_DOCKER_CACHE_VOLUME_SIZE_GB}GB {Settings.EBS_DOCKER_CACHE_VOLUME_TYPE} volume in {az}")
        resp = ec2.create_volume(
            AvailabilityZone=az,
            Size=Settings.EBS_DOCKER_CACHE_VOLUME_SIZE_GB,
            VolumeType=Settings.EBS_DOCKER_CACHE_VOLUME_TYPE,
            TagSpecifications=[
                {
                    "ResourceType": "volume",
                    "Tags": [
                        {"Key": TAG_NAME, "Value": f"{TAG_NAME_VALUE}-builder"},
                    ],
                }
            ],
        )
        volume_id = resp["VolumeId"]
        print(f"EBS docker cache: created volume [{volume_id}]")
        _wait_for_volume_status(ec2, volume_id, "available")

        # 2. Attach
        device = _find_free_device()
        print(f"EBS docker cache: attaching [{volume_id}] as [{device}]")
        ec2.attach_volume(
            Device=device,
            InstanceId=instance_id,
            VolumeId=volume_id,
        )
        _wait_for_volume_status(ec2, volume_id, "in-use")
        actual_device = _resolve_device(device)

        # 3. Format and mount
        Shell.check(f"sudo mkfs.ext4 -q {actual_device}", verbose=True, strict=True)
        Shell.check(f"sudo mkdir -p {_CACHE_MOUNT}", verbose=True, strict=True)
        Shell.check(
            f"sudo mount -o noatime {actual_device} {_CACHE_MOUNT}",
            verbose=True,
            strict=True,
        )
        mounted = True

        # 4. Start secondary dockerd
        print("EBS docker cache: starting secondary dockerd")
        Shell.check(
            f"sudo dockerd"
            f" --data-root {_CACHE_MOUNT}"
            f" --host unix://{_CACHE_DOCKERD_SOCK}"
            f" --pidfile {_CACHE_DOCKERD_PID}"
            f" --exec-root /var/run/docker-cache"
            f" >/dev/null 2>&1 &",
            verbose=True,
        )
        # Wait for the socket to appear
        for i in range(30):
            if os.path.exists(_CACHE_DOCKERD_SOCK):
                break
            time.sleep(1)
        else:
            raise RuntimeError("Secondary dockerd did not start within 30s")

        # Read the PID for cleanup
        if os.path.exists(_CACHE_DOCKERD_PID):
            with open(_CACHE_DOCKERD_PID, "r") as f:
                dockerd_pid = int(f.read().strip())

        # 5. Pull all images
        docker_cmd = f"sudo docker -H unix://{_CACHE_DOCKERD_SOCK}"
        for config in docker_configs:
            tag = f"{digests[config.name]}_{arch}"
            image_ref = f"{config.name}:{tag}"
            print(f"EBS docker cache: pulling [{image_ref}]")
            if not Shell.check(f"{docker_cmd} pull {image_ref}", verbose=True):
                print(f"WARNING: EBS docker cache: failed to pull [{image_ref}], skipping")

        # Show what we have
        Shell.check(f"{docker_cmd} images", verbose=True)

        # 6. Stop dockerd
        if dockerd_pid:
            print(f"EBS docker cache: stopping secondary dockerd (pid={dockerd_pid})")
            try:
                os.kill(dockerd_pid, signal.SIGTERM)
                for _ in range(30):
                    try:
                        os.kill(dockerd_pid, 0)
                        time.sleep(1)
                    except OSError:
                        break
            except OSError:
                pass
            dockerd_pid = None

        # 7. Unmount
        Shell.check(f"sudo umount {_CACHE_MOUNT}", verbose=True, strict=True)
        mounted = False

        # 8. Detach volume
        ec2.detach_volume(VolumeId=volume_id)
        _wait_for_volume_status(ec2, volume_id, "available")

        # 9. Create snapshot
        print(f"EBS docker cache: creating snapshot from [{volume_id}]")
        resp = ec2.create_snapshot(
            VolumeId=volume_id,
            Description=f"praktika-docker-cache {combined_digest} {arch}",
            TagSpecifications=[
                {
                    "ResourceType": "snapshot",
                    "Tags": [
                        {"Key": TAG_NAME, "Value": TAG_NAME_VALUE},
                        {"Key": TAG_DOCKER_CACHE_DIGEST, "Value": combined_digest},
                        {"Key": TAG_ARCH, "Value": arch},
                    ],
                }
            ],
        )
        snapshot_id = resp["SnapshotId"]
        print(f"EBS docker cache: snapshot [{snapshot_id}] creation initiated (async)")

        # 10. Delete the temporary volume (snapshot references S3 data, not the volume)
        ec2.delete_volume(VolumeId=volume_id)
        volume_id = None  # prevent double-delete in finally

        return snapshot_id

    except Exception as e:
        print(f"ERROR: EBS docker cache snapshot creation failed: {e}")
        traceback.print_exc()
        return None

    finally:
        # Cleanup on failure
        if dockerd_pid:
            try:
                os.kill(dockerd_pid, signal.SIGKILL)
            except OSError:
                pass
        if mounted:
            Shell.check(f"sudo umount {_CACHE_MOUNT}", verbose=True)
        if volume_id:
            try:
                ec2.detach_volume(VolumeId=volume_id)
                _wait_for_volume_status(ec2, volume_id, "available", timeout=60)
                ec2.delete_volume(VolumeId=volume_id)
            except Exception:
                print(f"WARNING: EBS docker cache: failed to clean up volume [{volume_id}]")


def mount_cache_volume(snapshot_id: str) -> Optional[str]:
    """Create a volume from snapshot, attach, mount at /var/lib/docker.

    Docker must be stopped before calling. Starts Docker after mounting.
    Returns volume_id on success (for cleanup), None on failure.
    """
    instance_id, az, region = _get_instance_metadata()
    ec2 = _ec2_client(region)

    volume_id = None
    device = None
    mounted = False

    try:
        # Stop Docker
        print("EBS docker cache: stopping Docker")
        Shell.check("sudo systemctl stop docker docker.socket containerd", verbose=True)

        # Create volume from snapshot
        print(f"EBS docker cache: creating volume from snapshot [{snapshot_id}] in [{az}]")
        resp = ec2.create_volume(
            SnapshotId=snapshot_id,
            AvailabilityZone=az,
            VolumeType=Settings.EBS_DOCKER_CACHE_VOLUME_TYPE,
            TagSpecifications=[
                {
                    "ResourceType": "volume",
                    "Tags": [
                        {"Key": TAG_NAME, "Value": f"{TAG_NAME_VALUE}-runner"},
                        {"Key": "instance", "Value": instance_id},
                    ],
                }
            ],
        )
        volume_id = resp["VolumeId"]
        _wait_for_volume_status(ec2, volume_id, "available")

        # Attach
        device = _find_free_device()
        print(f"EBS docker cache: attaching [{volume_id}] as [{device}]")
        ec2.attach_volume(
            Device=device,
            InstanceId=instance_id,
            VolumeId=volume_id,
        )
        _wait_for_volume_status(ec2, volume_id, "in-use")
        actual_device = _resolve_device(device)

        # Mount at /var/lib/docker
        Shell.check(f"sudo mkdir -p {_DOCKER_MOUNT}", verbose=True, strict=True)
        Shell.check(
            f"sudo mount -o noatime {actual_device} {_DOCKER_MOUNT}",
            verbose=True,
            strict=True,
        )
        mounted = True

        # Start Docker
        print("EBS docker cache: starting Docker with cached images")
        Shell.check("sudo systemctl start docker", verbose=True, strict=True)
        Shell.check("docker images", verbose=True)

        return volume_id

    except Exception as e:
        print(f"ERROR: EBS docker cache mount failed: {e}")
        traceback.print_exc()
        # Attempt recovery: unmount, restart Docker normally
        if mounted:
            Shell.check(f"sudo umount {_DOCKER_MOUNT}", verbose=True)
        Shell.check("sudo systemctl start docker", verbose=True)
        if volume_id:
            try:
                ec2.detach_volume(VolumeId=volume_id)
                _wait_for_volume_status(ec2, volume_id, "available", timeout=60)
                ec2.delete_volume(VolumeId=volume_id)
            except Exception:
                print(f"WARNING: EBS docker cache: failed to clean up volume [{volume_id}]")
        return None


def cleanup_cache_volume(volume_id: str) -> None:
    """Stop Docker, unmount /var/lib/docker, detach and delete the cache volume."""
    try:
        _, _, region = _get_instance_metadata()
    except Exception:
        region = Settings.AWS_REGION or "us-east-1"
    ec2 = _ec2_client(region)

    print(f"EBS docker cache: cleaning up volume [{volume_id}]")
    Shell.check("sudo systemctl stop docker docker.socket containerd", verbose=True)
    Shell.check(f"sudo umount {_DOCKER_MOUNT}", verbose=True)

    try:
        ec2.detach_volume(VolumeId=volume_id)
        _wait_for_volume_status(ec2, volume_id, "available", timeout=60)
        ec2.delete_volume(VolumeId=volume_id)
        print(f"EBS docker cache: volume [{volume_id}] deleted")
    except Exception as e:
        print(f"WARNING: EBS docker cache: cleanup failed for volume [{volume_id}]: {e}")


def cleanup_old_snapshots(arch: str, region: str, keep: int) -> None:
    """Delete old docker cache snapshots beyond the most recent `keep` per arch."""
    ec2 = _ec2_client(region)
    resp = ec2.describe_snapshots(
        Filters=[
            {"Name": f"tag:{TAG_NAME}", "Values": [TAG_NAME_VALUE]},
            {"Name": f"tag:{TAG_ARCH}", "Values": [arch]},
        ],
        OwnerIds=["self"],
    )
    snapshots = resp.get("Snapshots", [])
    if len(snapshots) <= keep:
        return

    # Sort newest first
    snapshots.sort(key=lambda s: s.get("StartTime", ""), reverse=True)
    to_delete = snapshots[keep:]
    for snap in to_delete:
        snap_id = snap["SnapshotId"]
        try:
            ec2.delete_snapshot(SnapshotId=snap_id)
            print(f"EBS docker cache: deleted old snapshot [{snap_id}]")
        except Exception as e:
            print(f"WARNING: EBS docker cache: failed to delete snapshot [{snap_id}]: {e}")
