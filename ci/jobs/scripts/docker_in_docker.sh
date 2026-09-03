#!/bin/bash
set -e

mkdir -p /etc/docker/
# No `exec-opts: native.cgroupdriver=systemd` here, deliberately: the `/docker` cap below binds
# only because the cgroupfs default parents containers there. Under the systemd driver they land
# elsewhere, so the cap would sit on an empty cgroup and containment would be silently absent.
echo '{
    "ipv6": true,
    "fixed-cidr-v6": "fd00::/8",
    "ip-forward": true,
    "log-level": "debug",
    "storage-driver": "overlay2",
    "features": {
        "containerd-snapshotter": false
    },
    "insecure-registries" : ["dockerhub-proxy.dockerhub-proxy-zone:5000"],
    "registry-mirrors" : ["http://dockerhub-proxy.dockerhub-proxy-zone:5000"],
    "log-driver": "json-file",
    "log-opts": {
        "max-size": "100m",
        "max-file": "3"
    }
}' | dd of=/etc/docker/daemon.json

# Split the job's `--memory` into three capped leaves (harness, daemon, nested containers), so an
# overrun hits a leaf. The RESERVES partition the limit and are validated below; `/init` is then
# capped at its ceiling instead, so the written caps can sum above it and the job cgroup can breach.
# Requested-then-required: a caller that asks for containment gets a refusal, not a daemon.
# BEGIN: cgroup containment
if [ "${CI_DIND_REQUIRE_CGROUP_CONTAINMENT:-0}" = 1 ]; then
    # Overridable so this block can run against a fake tree; production takes the default.
    cg=${CI_DIND_CGROUP_ROOT:-/sys/fs/cgroup}

    refuse() {
        echo "docker_in_docker.sh: cgroup containment is required but $1;" \
             "refusing to start an uncontained daemon" >&2
        exit 3
    }

    for var in CI_DIND_JOB_MEM CI_DIND_ROOT_RESERVE CI_DIND_INIT_RESERVE \
               CI_DIND_INIT_LIMIT CI_DIND_DAEMON_RESERVE CI_DIND_DAEMON_LIMIT \
               CI_DIND_NESTED_BUDGET; do
        case "${!var:-}" in
            "" | *[!0-9]*) refuse "\$$var is [${!var:-}], expected a byte count" ;;
        esac
    done

    # v2 keeps every controller in one tree; v1 mounts the memory controller on its own, so the
    # leaves live under `memory/` and the limit/usage files have different names.
    if [ -f "$cg/cgroup.controllers" ]; then
        cgroup_version=2
        leaf_root=$cg
        limit_file=memory.max
        # `0` is v2's sole hierarchy id in /proc/self/cgroup.
        own_cgroup=$(awk -F: '$1 == "0" { print $3; exit }' "${CI_DIND_PROC_CGROUP:-/proc/self/cgroup}")
    elif [ -d "$cg/memory" ]; then
        cgroup_version=1
        leaf_root=$cg/memory
        limit_file=memory.limit_in_bytes
        own_cgroup=$(awk -F: '$2 ~ /(^|,)memory(,|$)/ { print $3; exit }' \
                     "${CI_DIND_PROC_CGROUP:-/proc/self/cgroup}")
    else
        refuse "neither [$cg/cgroup.controllers] (cgroup v2) nor [$cg/memory] (cgroup v1) exists"
    fi

    # Every leaf path below is unqualified, so it must be the container's own cgroup: true only
    # under `--cgroupns=private`. Under `--cgroupns=host` these are the HOST's root, and
    # writing there would migrate every host process, the runner agent included.
    [ "$own_cgroup" = "/" ] || \
        refuse "the cgroup namespace is not private (own cgroup is [$own_cgroup], expected [/])"

    # Confirms both that the namespace root is this limited container (a host root reports
    # `max` on v2 and a near-INT64_MAX sentinel on v1) and that the limit the job requested
    # actually applied.
    root_memory_max=$(cat "$leaf_root/$limit_file")
    [ "$root_memory_max" = "$CI_DIND_JOB_MEM" ] || \
        refuse "the root $limit_file is [$root_memory_max], expected the job limit [$CI_DIND_JOB_MEM]"

    # Reserves that leave the containers nothing would otherwise surface only as a confusing
    # negative `memory.max` write. The reserves are absolute, so the usual cause is a host too
    # small to hold them: name the shortfall, or it reads as a harness bug.
    reserves_gib=$(( (CI_DIND_ROOT_RESERVE + CI_DIND_INIT_RESERVE + CI_DIND_DAEMON_RESERVE) / 1024**3 ))
    [ "$CI_DIND_NESTED_BUDGET" -gt 0 ] || \
        refuse "the nested budget is [$CI_DIND_NESTED_BUDGET] bytes, leaving the test containers \
nothing: this host offers a $(( CI_DIND_JOB_MEM / 1024**3 )) GiB job limit and the reserves need \
$reserves_gib GiB, so integration tests need a larger host, or unset \
CI_DIND_REQUIRE_CGROUP_CONTAINMENT to run uncontained"

    # Each reserve is bounded by the job limit before they are summed: the sum is signed 64-bit,
    # so a value near its top wraps negative and passes the total check below.
    for var in CI_DIND_ROOT_RESERVE CI_DIND_INIT_RESERVE CI_DIND_DAEMON_RESERVE \
               CI_DIND_NESTED_BUDGET CI_DIND_INIT_LIMIT CI_DIND_DAEMON_LIMIT; do
        [ "${!var}" -le "$CI_DIND_JOB_MEM" ] || \
            refuse "\$$var is [${!var}] bytes, above the job limit of $CI_DIND_JOB_MEM"
    done

    # The RESERVES may not promise more than the job has, or the outer limit could still fire
    # first. ROOT_RESERVE is not a leaf: it covers what stays charged to the root, which on v2
    # is the pages faulted before delegation, unmigratable and unreclaimable.
    reserved=$(( CI_DIND_ROOT_RESERVE + CI_DIND_INIT_RESERVE + CI_DIND_DAEMON_RESERVE + CI_DIND_NESTED_BUDGET ))
    [ "$reserved" -le "$CI_DIND_JOB_MEM" ] || \
        refuse "the reserves total $reserved bytes, above the job limit of $CI_DIND_JOB_MEM"

    # The init limit overlaps `/docker`'s reserve, so what bounds it is the leaves it does not
    # overlap: it must be at least its own reserve and still leave room for the other two.
    [ "$CI_DIND_INIT_LIMIT" -ge "$CI_DIND_INIT_RESERVE" ] || \
        refuse "the init limit is [$CI_DIND_INIT_LIMIT] bytes, below its own reserve of [$CI_DIND_INIT_RESERVE]"
    init_headroom=$(( CI_DIND_INIT_LIMIT + CI_DIND_ROOT_RESERVE + CI_DIND_DAEMON_RESERVE ))
    [ "$init_headroom" -le "$CI_DIND_JOB_MEM" ] || \
        refuse "the init limit of $CI_DIND_INIT_LIMIT bytes plus the root and daemon reserves \
totals $init_headroom, above the job limit of $CI_DIND_JOB_MEM"

    # The daemon limit is bounded the same way, against the leaves it does not overlap.
    [ "$CI_DIND_DAEMON_LIMIT" -ge "$CI_DIND_DAEMON_RESERVE" ] || \
        refuse "the daemon limit is [$CI_DIND_DAEMON_LIMIT] bytes, below its own reserve of [$CI_DIND_DAEMON_RESERVE]"
    daemon_headroom=$(( CI_DIND_DAEMON_LIMIT + CI_DIND_ROOT_RESERVE + CI_DIND_INIT_RESERVE ))
    [ "$daemon_headroom" -le "$CI_DIND_JOB_MEM" ] || \
        refuse "the daemon limit of $CI_DIND_DAEMON_LIMIT bytes plus the root and init reserves \
totals $daemon_headroom, above the job limit of $CI_DIND_JOB_MEM"

    mkdir -p "$leaf_root/init" "$leaf_root/dockerd" "$leaf_root/docker"

    if [ "$cgroup_version" = 2 ]; then
        # Delegation fails with EBUSY while the cgroup holds any process, and the caller keeps
        # forking (it polls `docker info`), so retry the move-then-enable pair rather than doing
        # it once. Failing to move a pid that has already exited is expected. v1 has no
        # `subtree_control` and no such constraint, so it needs none of this.
        delegated=0
        for _ in $(seq 1 20); do
            xargs -rn1 < "$cg/cgroup.procs" > "$cg/init/cgroup.procs" 2>/dev/null || :
            if sed -e 's/ / +/g' -e 's/^/+/' < "$cg/cgroup.controllers" \
                    > "$cg/cgroup.subtree_control" 2>/dev/null; then
                delegated=1
                break
            fi
            sleep 0.5
        done
        [ "$delegated" = 1 ] || \
            refuse "enabling controllers on [$cg/cgroup.subtree_control] kept failing, with pids [$(tr '\n' ' ' < "$cg/cgroup.procs")] still in the root cgroup"
    fi

    # Cap every leaf before the daemon starts; dockerd recreates `/docker` on startup, and the
    # limit is measured to survive that only in this order.
    echo "$CI_DIND_INIT_LIMIT"     > "$leaf_root/init/$limit_file"    || refuse "capping [$leaf_root/init] failed"
    echo "$CI_DIND_DAEMON_LIMIT"   > "$leaf_root/dockerd/$limit_file" || refuse "capping [$leaf_root/dockerd] failed"
    echo "$CI_DIND_NESTED_BUDGET"  > "$leaf_root/docker/$limit_file"  || refuse "capping [$leaf_root/docker] failed"

    # A memory cap alone bounds resident pages, so a leaf can still exceed its advertised budget
    # by swapping. v1 spells this `memory.memsw.limit_in_bytes` and counts memory+swap against one
    # number; v2 has a separate `memory.swap.max` that defaults to unlimited. Either file is absent
    # unless the kernel accounts swap, and on a swapless host the memory cap is already the whole
    # limit, so a leaf that cannot be swap-limited is only fatal when swap exists.
    # `CI_DIND_PROC_MEMINFO` exists for the same reason as the cgroup overrides above: a test
    # cannot add swap to the machine it runs on. Left empty when the field cannot be read, so that
    # only an observed zero excuses a failed swap write; treating unknown as zero would let every
    # write fail and still start the daemon.
    swap_total=$(awk '/^SwapTotal:/ { print $2; found = 1 } END { exit !found }' \
                 "${CI_DIND_PROC_MEMINFO:-/proc/meminfo}" 2>/dev/null) || swap_total=""
    for leaf in init dockerd docker; do
        case $leaf in
            init)    bytes=$CI_DIND_INIT_LIMIT ;;
            dockerd) bytes=$CI_DIND_DAEMON_LIMIT ;;
            docker)  bytes=$CI_DIND_NESTED_BUDGET ;;
        esac
        if [ "$cgroup_version" = 1 ]; then
            swap_file=memory.memsw.limit_in_bytes
            swap_bytes=$bytes
        else
            # v2 counts swap separately from `memory.max`, so 0 keeps the leaf's total at its cap.
            swap_file=memory.swap.max
            swap_bytes=0
        fi
        echo "$swap_bytes" > "$leaf_root/$leaf/$swap_file" 2>/dev/null || \
            [ "$swap_total" = 0 ] || \
            refuse "capping swap for [$leaf_root/$leaf] via [$swap_file] failed and the host has \
[${swap_total:-unknown}] kB of swap, so the leaf could exceed its budget by swapping"
    done

    if [ "$cgroup_version" = 1 ]; then
        # v2 migrates these as a side effect of delegation; v1 needs it done explicitly, or the
        # harness (pytest, its xdist workers, the coverage merge) keeps running in the uncapped
        # root and an overrun there hits the job's own limit instead of `/init`. After the caps,
        # so nothing lands in a leaf that is still uncapped.
        root_pids=$(wc -l < "$leaf_root/cgroup.procs")
        xargs -rn1 < "$leaf_root/cgroup.procs" > "$leaf_root/init/cgroup.procs" 2>/dev/null || :
        # A pid that exited mid-move is expected, so this asserts the leaf received SOMETHING
        # rather than an exact count: an empty `/init` after a non-empty root means the write
        # itself failed and the harness is still uncapped.
        if [ "$root_pids" != 0 ] && [ "$(wc -l < "$leaf_root/init/cgroup.procs")" = 0 ]; then
            refuse "moving [$root_pids] process(es) from [$leaf_root] into [$leaf_root/init] left it empty"
        fi
    fi

    # Run the daemon from its own leaf so an overrun by the nested containers, whose OOM scan
    # covers the offending cgroup and its descendants, cannot pick it as the victim.
    echo $$ > "$leaf_root/dockerd/cgroup.procs" || refuse "moving this shell into [$leaf_root/dockerd] failed"

    echo "docker_in_docker.sh: cgroup containment active (cgroup v$cgroup_version):" \
         "init=$CI_DIND_INIT_LIMIT dockerd=$CI_DIND_DAEMON_LIMIT" \
         "docker=$CI_DIND_NESTED_BUDGET within $CI_DIND_JOB_MEM"
fi
# END: cgroup containment

# Binding to an IP address without --tlsverify is deprecated. Startup is intentionally being slowed
# unless --tls=false or --tlsverify=false is set
#
# In case of test hung it is convenient to use pytest --pdb to debug it,
# and on hung you can simply press Ctrl-C and it will spawn a python pdb,
# but on SIGINT dockerd will exit, so we spawn new session to ignore SIGINT by
# docker.
# Note, that if you will run it via runner, it will send SIGINT to docker anyway.
setsid dockerd --host=unix:///var/run/docker.sock --tls=false --host=tcp://0.0.0.0:2375 --default-address-pool base=172.17.0.0/12,size=24
