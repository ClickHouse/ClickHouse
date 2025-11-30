import sys
from pathlib import Path

from ci.praktika.info import Info


def has_new_functional_tests(changed_files):
    for file in changed_files:
        file = file.removeprefix(".").removeprefix("/")
        if file.startswith("tests/queries/0_stateless"):
            return True
    return False


def has_new_integration_tests(changed_files):
    for file in changed_files:
        file = file.removeprefix(".").removeprefix("/")
        if (
            file.startswith("tests/integration/test_")
            and Path(file).name.startswith("test")
            and file.endswith(".py")
        ):
            return True
    return False


def has_new_unit_tests(changed_files):
    for file in changed_files:
        file = file.removeprefix(".").removeprefix("/")
        if file.startswith("src") and "/tests/" in file:
            return True
    return False


def check():
    if not " Bug Fix" in Info().pr_body:
        print("Not a bug fix PR - skip")
        return True

    changed_files = Info().get_changed_files()
    if (
        not has_new_unit_tests(changed_files)
        and not has_new_functional_tests(changed_files)
        and not has_new_integration_tests(changed_files)
    ):
        print(f"No new tests have been added")
        return False
    return True


if __name__ == "__main__":
    if not check():
        sys.exit(1)
