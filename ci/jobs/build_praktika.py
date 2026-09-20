#!/usr/bin/env python3

import argparse
import subprocess
import sys
import os
from pathlib import Path

VENV_DIR = Path(".venv-pypy")
PRAKTIKA_DIR = Path("ci/praktika")


def run(cmd, check=True):
    print(f"\n>>> Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=check)


def run_env(cmd, check=True, env=None):
    print(f"\n>>> Running: {' '.join(cmd)}")
    env_copy = os.environ.copy()

    for key, value in (env or {}).items():
        env_copy[key] = value
    subprocess.run(cmd, check=check, env=env_copy)


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
    if (PRAKTIKA_DIR / "requirements.txt").exists():
        run([str(venv_pip()), "install", "-r", str(PRAKTIKA_DIR / "requirements.txt")])


def build_package(token: str):
    run([str(venv_python()), "-m", "build", str(PRAKTIKA_DIR)])

    dist_files = [str(f) for f in (PRAKTIKA_DIR / "dist").glob("*")]
    run([str(venv_python()), "-m", "twine", "check", *dist_files])

    run_env(
        [str(venv_python()), "-m", "twine", "upload", *dist_files],
        env={
            "TWINE_USERNAME": "__token__",
            "TWINE_PASSWORD": token if token else os.getenv("TWINE_PASSWORD"),
        },
    )


def main():
    parser = argparse.ArgumentParser(description="Upload package to PyPI using a token")
    parser.add_argument("--token", help="PyPI API token")
    args = parser.parse_args()

    install_packages()
    ensure_venv()
    install_dependencies()
    build_package(token=args.token)
    print("\nBuild completed successfully.")


if __name__ == "__main__":
    main()
