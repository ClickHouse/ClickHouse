import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--builddir", action="store", default=None, help="Path to build directory to use binaries from",
    )


@pytest.fixture
def cmdopts(request):
    return {
        'builddir': request.config.getoption("--builddir"),
    }
