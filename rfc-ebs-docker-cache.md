# EBS Snapshot Docker Cache for CI Runners

## How It Works

### Snapshot Creation (in Docker build jobs)

1. CI config job computes a combined SHA256 digest from all per-image digests → stored in `RunConfig.docker_cache_digest`
2. ARM and AMD docker build jobs each build and push images to DockerHub as usual
3. After building, if `ENABLE_EBS_DOCKER_CACHE` is set:
   - Check if a snapshot with matching `docker-cache-digest` + `arch` tag already exists → skip if yes
   - Clean up buildx cache to free disk space
   - Create a 100 GB gp3 EBS volume, attach, format ext4
   - Stop Docker, bind-mount EBS subdirs to `/var/lib/docker` and `/var/lib/containerd`
   - Pull all just-built images from DockerHub into the EBS-backed Docker
   - Stop Docker, unmount, detach, create EBS snapshot (async), delete volume
4. Snapshot is tagged with `docker-cache-digest=<combined_digest>`, `arch=<amd|arm>`

### Snapshot Consumption (on runners)

1. Before running a Docker job, runner computes combined digest from `RunConfig`
2. Calls `ec2:DescribeSnapshots` filtered by digest + arch + `status=completed`
3. If found: creates volume from snapshot, attaches, bind-mounts `/var/lib/docker` and `/var/lib/containerd`, starts Docker → all images are present, zero pull
4. If not found: falls back to normal Docker pull (transparent, non-fatal)
5. After job: stops Docker, unmounts, detaches, deletes volume

### First Run After Image Change

The snapshot is created asynchronously (takes ~10-15 min). Jobs in the same CI run will still pull from DockerHub. The **next** CI run with the same images will find the completed snapshot and skip pulling.

## Docker Images

### CI-built images (in EBS snapshot, 34 images)

```
clickhouse/arrowflight-server-test   clickhouse/mysql-golang-client
clickhouse/binary-builder            clickhouse/mysql-java-client
clickhouse/cctools                   clickhouse/mysql-js-client
clickhouse/docs-builder              clickhouse/mysql-php-client
clickhouse/dotnet-client             clickhouse/mysql_dotnet_client
clickhouse/fasttest                  clickhouse/nginx-dav
clickhouse/fuzzer                    clickhouse/performance-comparison
clickhouse/install-deb-test          clickhouse/postgresql-java-client
clickhouse/install-rpm-test          clickhouse/python-bottle
clickhouse/integration-helper        clickhouse/s3-proxy
clickhouse/integration-test          clickhouse/server-jepsen-test
clickhouse/integration-test-with-hms clickhouse/sqlancer-test
clickhouse/integration-test-with-unity-catalog
clickhouse/integration-tests-runner  clickhouse/stateless-test
clickhouse/keeper-jepsen-test        clickhouse/stress-test
clickhouse/kerberos-kdc              clickhouse/style-test
clickhouse/test-base                 clickhouse/test-mysql57
clickhouse/test-mysql80
```

### External images (NOT in EBS snapshot yet — TODO)

These are pulled by integration tests at runtime. Adding them to the snapshot would eliminate all Docker pulls.

```
bitnamilegacy/openldap:2.6.8        mcr.microsoft.com/azure-storage/azurite:3.35.0
cassandra:4.0                        minio/mc:RELEASE.2025-04-16T18-13-26Z
confluentinc/cp-kafka:5.2.0         minio/minio:RELEASE.2024-07-31T05-46-26Z
confluentinc/cp-kafka:7.9.0         minio/minio:RELEASE.2024-09-13T20-26-02Z
confluentinc/cp-schema-registry:5.2.0  mongo:6.0
confluentinc/cp-zookeeper:5.2.0     motoserver/moto:5.1.5
coredns/coredns:1.9.3               mysql:8.0
curlimages/curl                      nats
dremio/dremio-oss:26.0              postgres:12.2-alpine
ghcr.io/letsencrypt/pebble:2.9.0   postgres:16
ghcr.io/letsencrypt/pebble-challtestsrv:2.9.0  prom/prometheus:v3.5.0
ghcr.io/projectnessie/nessie:0.107.2  rabbitmq:4.2.1-management-alpine
ghcr.io/ytsaurus/local:stable-24.2  redis
lgboustc/hive_test:v2.0             tabulario/iceberg-rest:1.6.0
tabulario/spark-iceberg:3.5.5_1.8.1 vakamo/lakekeeper:v0.9.4
willfarrell/autoheal:1.2.0          zookeeper:3.4.9
zookeeper:3.6.2
```

Note: `clickhouse/jdbc-bridge:2.1.0-dirty` is in compose files but not in `defs.py` (likely deprecated).

## EBS Volume Size

Current default: **100 GB gp3** per snapshot. 34 CI images compressed in overlay2 format. To be increased if external images are added (~30 more images). Volume size is the primary cost driver — reducing it to actual image footprint is the main optimization lever.

## Snapshot Cleanup

### Current implementation

After creating a new snapshot, delete old ones beyond `EBS_DOCKER_CACHE_MAX_SNAPSHOTS_PER_ARCH` (default: 3) per architecture. This keeps the most recent 3 snapshots per arch.

### TODO: time-based retention

Ideal policy: delete snapshots not used for 1 month. AWS provides `DescribeSnapshotAttribute` and EBS Snapshots Archive tracks last access metadata (`LastAccessed` via CloudWatch `VolumeReadOps` on volumes created from a snapshot, or via `DescribeSnapshots` `StorageTier` / access tracking). Implementation:
1. Enable EBS snapshot access tracking (if available in the region) or query CloudWatch for `CreateVolume` events referencing each snapshot
2. Periodic cleanup (Lambda or cron): delete snapshots not accessed for 30 days
3. Old release branch snapshots expire naturally without manual intervention

## Configuration

```python
# ci/settings/settings.py
ENABLE_EBS_DOCKER_CACHE = True
```

```python
# ci/praktika/settings.py (defaults)
EBS_DOCKER_CACHE_VOLUME_SIZE_GB = 100
EBS_DOCKER_CACHE_VOLUME_TYPE = "gp3"
EBS_DOCKER_CACHE_MAX_SNAPSHOTS_PER_ARCH = 3
```

### IAM permissions required

`ec2:AttachVolume`, `ec2:CreateSnapshot`, `ec2:CreateVolume`, `ec2:DeleteSnapshot`, `ec2:DeleteVolume`, `ec2:DetachVolume`, `ec2:CreateTags`.

## Files

- `ci/praktika/ebs_docker_cache.py` — EBS snapshot operations (create, find, mount, cleanup)
- `ci/praktika/settings.py` — settings
- `ci/praktika/runtime.py` — `docker_cache_digest` field in `RunConfig`
- `ci/praktika/native_jobs.py` — snapshot creation in docker build jobs
- `ci/praktika/runner.py` — snapshot mount/cleanup on runners

## TODO

- **Per-image or per-group snapshots**: Instead of one large snapshot with all 34 images, create smaller snapshots per image or per job group (e.g., one for integration test images, one for build images). This reduces EBS volume size per runner and lowers cost significantly.
- **Docker-in-Docker**: Integration tests run Docker inside Docker with a separate named volume (`clickhouse_integration_tests_volume:/var/lib/docker`). The inner Docker daemon does not see the host EBS cache, so all images are still pulled inside the container. Solving this requires either sharing the host Docker socket, using overlayfs layers, or pre-loading images into the inner daemon.
- Add external Docker images to the snapshot (eliminates all pulls for integration tests)
- Time-based snapshot retention (1 month TTL)
- Monitor snapshot sizes and adjust volume size
- Consider Fast Snapshot Restore (FSR) if lazy-load latency is a problem
