from ci.praktika.utils import Shell


def check():
    Shell.check("docker system prune -a -f", verbose=True)
    return True


if __name__ == "__main__":
    check()
