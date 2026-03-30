from ci.praktika.utils import Shell

Shell.run("docker ps -a; docker container prune -f; docker volume prune -f --all; docker system df")
