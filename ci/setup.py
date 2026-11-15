from setuptools import find_packages, setup

setup(
    name="praktika",
    version="0.1",
    packages=find_packages(),
    url="https://github.com/ClickHouse/praktika",
    license="Apache 2.0",
    author="Max Kainov",
    author_email="max.kainov@clickhouse.com",
    description="CI Infrastructure Toolbox",
    entry_points={
        "console_scripts": [
            "praktika=praktika.__main__:main",
        ]
    },
)
