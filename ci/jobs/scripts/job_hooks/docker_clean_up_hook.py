from ci.praktika.utils import Shell


def check():
    print("Clean up build cache")
    Shell.check("docker builder prune -a -f", verbose=True)
    print("Clean up stopped containers")
    Shell.check("docker container prune -f", verbose=True)

    print("Clean up non-latest images per each Repository")
    Shell.check(
        "docker images --format '{{.Repository}} {{.ID}} {{.CreatedAt}}' "
        " | sort -k1,1 -k3,3M -k4,4n -k5,5n "
        " | tac "
        " | awk '!seen[$1]++ {next} {print $2}' "
        " | xargs -r docker rmi",
        verbose=True,
    )
    return True


if __name__ == "__main__":
    check()
