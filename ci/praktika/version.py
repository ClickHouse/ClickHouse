import re
from importlib.metadata import PackageNotFoundError, version as package_version
from pathlib import Path
from typing import Optional, Tuple


def _version_from_pyproject(pyproject: Optional[Path] = None) -> str:
    if pyproject is None:
        pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    if not pyproject.is_file():
        return ""

    in_project = False
    for raw_line in pyproject.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line == "[project]":
            in_project = True
            continue
        if in_project and line.startswith("["):
            return ""
        if in_project and line.startswith("version"):
            match = re.match(r'version\s*=\s*["\']([^"\']+)["\']', line)
            if match:
                return match.group(1)
    return ""


def current_praktika_version() -> str:
    return _version_from_pyproject() or package_version("praktika")


def current_praktika_controller_version(pyproject: Optional[Path] = None) -> str:
    if pyproject is None:
        pyproject = Path(__file__).resolve().parents[1] / "bootstrap" / "pyproject.toml"
    return _version_from_pyproject(pyproject) or package_version("praktika-controller")


def version_key(value: str) -> Tuple[int, ...]:
    parts = str(value).strip().split(".")
    if not parts or any(not part.isdigit() for part in parts):
        raise ValueError(f"unsupported version format: {value!r}")
    return tuple(int(part) for part in parts)


def compat_version(value: str) -> str:
    parts = version_key(value)
    if len(parts) < 2:
        raise ValueError(f"unsupported compat version format: {value!r}")
    return ".".join(str(part) for part in parts[:2])


try:
    __version__ = current_praktika_version()
except PackageNotFoundError:
    __version__ = "0.0.0"
