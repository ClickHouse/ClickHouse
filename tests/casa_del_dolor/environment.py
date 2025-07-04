import random
import subprocess
import sys
import zoneinfo


def get_system_locales(logger) -> list[str]:
    try:
        if sys.platform.startswith("linux") or sys.platform == "darwin":
            # Linux/macOS
            result = subprocess.run(["locale", "-a"], capture_output=True, text=True)
            return result.stdout.strip().split("\n")
        if sys.platform.startswith("win"):
            # Windows - different approach needed
            result = subprocess.run(
                ["powershell", "Get-WinSystemLocale"], capture_output=True, text=True
            )
            return result.stdout.strip().split("\n")
        logger.error("Couldn't detect running OS")
    except Exception as e:
        logger.warn(f"Error getting system locales: {e}")
    return []


def get_system_timezones() -> list[str]:
    return list(zoneinfo.available_timezones())


# Set environment variables for cluster
def set_environment_variables(logger, args, process_name: str) -> dict[str, str]:
    test_env_variables: dict[str, str] = {}

    possible_locales: list[str] = (
        get_system_locales(logger)
        if random.randint(1, 100) <= args.set_locales_prob
        else []
    )
    if len(possible_locales) > 0:
        test_env_variables["LC_ALL"] = random.choice(possible_locales)
        if random.randint(1, 100) <= 70:
            test_env_variables["LANG"] = random.choice(possible_locales)
        if random.randint(1, 100) <= 70:
            test_env_variables["LC_CTYPE"] = random.choice(possible_locales)

    possible_timezones: list[str] = (
        get_system_timezones()
        if random.randint(1, 100) <= args.set_timezones_prob
        else []
    )
    if len(possible_timezones) > 0:
        test_env_variables["TZ"] = random.choice(possible_timezones)

    if len(test_env_variables) > 0:
        logger.info(
            f"Setting environment variable(s) for {process_name}: {' '.join([f"{key}={value}" for key, value in test_env_variables.items()])}"
        )

    return test_env_variables
