
import sys
from pathlib import Path

repo_path = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.append(str(repo_path))
from ci.praktika.utils import Shell

Shell.run("docker ps -a; docker container prune -f; docker volume prune -f --all; docker system df")
