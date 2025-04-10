from ci.praktika.utils import Shell


def check():
    print("Clean up build cache")
    Shell.check("docker builder prune -a -f", verbose=True)
    print("Clean up stopped containers")
    Shell.check("docker container prune -f", verbose=True)

    print("Clean up non-latest images per each Repository")
    Shell.check(
        "docker images --format '{{.Repository}} {{.Tag}} {{.ID}}' "
        " | sort -u -k1,1 | awk '{print $3}' "
        " | xargs -r -I {} docker images --filter 'before={}' --quiet "
        " | sort -u | xargs -r docker rmi",
        verbose=True,
    )
    return True


if __name__ == "__main__":
    check()
