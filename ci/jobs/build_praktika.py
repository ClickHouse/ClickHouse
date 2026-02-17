#!/usr/bin/env python3

import subprocess
import sys
import os
from pathlib import Path

VENV_DIR = Path(".venv-pypy")


def run(cmd, check=True):
    print(f"\n>>> Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=check)


def ensure_venv():
    if not VENV_DIR.exists():
        print("Creating PyPy virtual environment...")
        run([sys.executable, "-m", "venv", str(VENV_DIR)])
    else:
        print("Virtual environment already exists.")


def venv_python():
    return VENV_DIR / "bin" / "python"


def venv_pip():
    return VENV_DIR / "bin" / "pip"


def install_packages():
    run(["sudo", "apt-get", "update"])
    run(["sudo", "apt-get", "install", "-y", "python3-pip", "python3-venv"])


def install_dependencies():
    run([str(venv_pip()), "install", "--upgrade", "pip", "build", "twine"])
    if Path("requirements.txt").exists():
        run([str(venv_pip()), "install", "-r", "requirements.txt"])


def build_package():
    run([str(venv_python()), "-m", "build", "ci/praktika"])
    run([str(venv_python()), "-m", "twine", "check", "dist/*"])
    run([str(venv_python()), "-m", "twine", "upload", "dist/*"])


def main():
    install_packages()
    ensure_venv()
    install_dependencies()
    build_package()
    print("\nBuild completed successfully.")


if __name__ == "__main__":
    main()
