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
    Shell.check("docker builder prune -a -f", verbose=True)
    print("Clean up stopped containers")
    Shell.check("docker container prune -f", verbose=True)
    # Without `-f` it only asks for a confirmation on a terminal that is not there, and
    # exits without deleting anything.
    Shell.check("docker system prune -f", verbose=True)
    return True


if __name__ == "__main__":
    check()
