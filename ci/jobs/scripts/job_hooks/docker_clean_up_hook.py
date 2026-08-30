from ci.praktika.utils import Shell


def check():
    print(
        "Remove all images with names starting with clickhouse/clickhouse- or clickhouse/install- (forced)"
    )
    Shell.check(
        "docker images --format '{{.Repository}}:{{.Tag}}' | grep -E '^clickhouse/(clickhouse|install)-' | xargs -r docker rmi -f",
        verbose=True,
    )
    print("Clean up non-latest images per each Repository")
    # `{{.CreatedAt}}` is an ISO date, so `sort -M` compares every date equal and the order
    # falls through to the hour. Remove by reference: `docker rmi <id>` without `-f` refuses
    # an id that several repositories still tag.
    Shell.check(
        "docker images --format '{{.Repository}}:{{.Tag}} {{.Repository}} {{.CreatedAt}}' "
        " | sort -k2,2 -k3,3r -k4,4r "
        " | awk '!seen[$2]++ {next} {print $1}' "
        " | xargs -r docker rmi",
        verbose=True,
    )
    print("Clean up build cache")
    # A `docker-container` builder keeps its cache in its own buildkit container, out of
    # reach of the prunes below, and `buildx ls` has no --all-builders equivalent. Unindented
    # rows are the builders; a builder that never built has no container yet, hence `|| true`.
    Shell.check(
        "docker buildx ls "
        " | awk 'NR>1 && $0 !~ /^[[:space:]]/ && NF { sub(/[*]$/, \"\", $1); print $1 }' "
        " | xargs -r -I% sh -c 'docker buildx prune -a -f --builder % || true'",
        verbose=True,
    )
    # `docker builder prune` talks to the daemon, so it only ever reclaims the cache of the
    # `docker` driver builder - never that of a `docker-container` one, which is why it
    # reports `0B` on a host where a multi-platform build has just written gigabytes.
    Shell.check("docker builder prune -a -f", verbose=True)
    print("Clean up orphaned buildx builders")
    # The runner removes every container between jobs, which leaves a `docker-container`
    # builder registered with no buildkit container behind it. The prune above then fails
    # with `No such container: buildx_buildkit_<node>`, and the cache - a named
    # `buildx_buildkit_<node>_state` volume that no prune below touches - stays on disk and
    # grows with every docker image build the host runs. Drop such a builder so the next
    # build starts from a fresh one; `--all-inactive` keeps a builder whose container is
    # still up, so a live cache is pruned above rather than thrown away here.
    Shell.check("docker buildx rm --all-inactive --force", verbose=True)
    # `docker buildx rm` leaves the volume behind when the container is already gone, so
    # remove it by name. A volume still attached to a running builder is refused, which is
    # what keeps this from deleting a live cache.
    Shell.check(
        "docker volume ls --quiet "
        " | grep buildx_buildkit "
        " | xargs -r -I% sh -c 'docker volume rm % || true'",
        verbose=True,
    )
    print("Clean up stopped containers")
    Shell.check("docker container prune -f", verbose=True)
    # Without `-f` it only asks for a confirmation on a terminal that is not there, and
    # exits without deleting anything. Note that it does not reclaim volumes.
    Shell.check("docker system prune -f", verbose=True)
    # The job that runs out of disk space is rarely the job that filled it, so leave behind
    # what docker still holds - `Local Volumes` is the row the prunes above cannot reclaim.
    print("Docker disk usage after the clean up")
    Shell.check("docker system df", verbose=True)
    return True


if __name__ == "__main__":
    check()
