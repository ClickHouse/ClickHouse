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
    # A `docker-container` builder keeps its cache in a named `buildx_buildkit_<node>_state`
    # volume, out of reach of the prunes below, and `buildx ls` has no --all-builders
    # equivalent. Unindented rows are the builders, indented `\\_ <node>` ones their nodes.
    returncode, buildx_ls, _ = Shell.get_res_stdout_stderr("docker buildx ls")
    builders = []
    nodes = []
    for line in buildx_ls.splitlines()[1:]:  # the first row is the header
        if not line.strip():
            continue
        if line[0].isspace():
            fields = line.split()
            if len(fields) > 1 and fields[0] == "\\_":
                nodes.append(fields[1])
        else:
            builders.append(line.split()[0].rstrip("*"))
    for builder in builders:
        # The runner removes every container between jobs, so by the time the next job runs
        # the builder is `inactive` and the prune fails with
        # `No such container: buildx_buildkit_<node>`, leaving a cache that grows with every
        # docker image build the host runs - the reason a style-checker host ends up with no
        # free disk. `--bootstrap` starts the container back up, which makes the cache
        # reachable again, so the prune empties the volume in place and the builder itself
        # stays registered for the docker image jobs that expect it.
        Shell.check(f"docker buildx inspect {builder} --bootstrap", verbose=True)
        Shell.check(f"docker buildx prune -a -f --builder {builder}", verbose=True)
    # `docker builder prune` talks to the daemon, so it only ever reclaims the cache of the
    # `docker` driver builder - never that of a `docker-container` one, which is why it
    # reports `0B` on a host where a multi-platform build has just written gigabytes.
    Shell.check("docker builder prune -a -f", verbose=True)
    print("Remove the state volumes of buildx nodes that no longer exist")
    # A builder removed without its volume leaves a `buildx_buildkit_<node>_state` volume with
    # nothing able to reach it: no prune above touches a volume, and `docker system prune`
    # skips volumes without `--volumes`. Remove a volume only when its exact node is not one
    # of the nodes listed above - a node name is not a prefix match on a builder name, or
    # `buildx_buildkit_default-builder0_state` would look reachable through the unrelated
    # `default` builder. `docker volume rm` also refuses a volume attached to a running
    # container, so a cache that is still in use cannot be thrown away here.
    if returncode != 0 or not nodes:
        # Fail closed: an empty list of nodes would make every volume look orphaned.
        print(f"WARNING: cannot list the buildx nodes (exit code {returncode}), skip")
    else:
        volumes = [
            line.strip()
            for line in Shell.get_output(
                "docker volume ls --quiet --filter name=buildx_buildkit"
            ).splitlines()
            if line.strip().startswith("buildx_buildkit_")
            and line.strip().endswith("_state")
        ]
        for volume in volumes:
            node = volume[len("buildx_buildkit_") : -len("_state")]
            if node in nodes:
                continue
            Shell.check(f"docker volume rm {volume}", verbose=True)
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
