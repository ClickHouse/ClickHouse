How to build ClickHouse on Mac OS X
===================================

Build should work on Mac OS X 10.12. If you're using earlier version, you can try to build ClickHouse using Gentoo Prefix and clang sl in this instruction.
With appropriate changes, build should work on any other OS X distribution.

Install Homebrew
----------------

.. code-block:: bash

    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"


Install required compilers, tools, libraries
--------------------------------------------

.. code-block:: bash

    brew install cmake gcc icu4c mysql openssl unixodbc libtool gettext homebrew/dupes/zlib readline boost --cc=gcc-7


Checkout ClickHouse sources
---------------------------

To get the latest stable version:

.. code-block:: bash

    git clone -b stable --recursive --depth=10 git@github.com:yandex/ClickHouse.git
    # or: git clone -b stable --recursive --depth=10 https://github.com/yandex/ClickHouse.git

    cd ClickHouse

For development, switch to the ``master`` branch.
For the latest release candidate, switch to the ``testing`` branch.

Build ClickHouse
----------------

.. code-block:: bash

    mkdir build
    cd build
    cmake .. -DCMAKE_CXX_COMPILER=`which g++-7` -DCMAKE_C_COMPILER=`which gcc-7`
    make -j `sysctl -n hw.ncpu`
    cd ..

If you're using macOS 10.13 High Sierra, it's not possible to build it with GCC-7 due to `a bug <https://github.com/yandex/ClickHouse/issues/1474>`_. Build it with Clang 5.0 instead:

.. code-block:: bash

    brew install llvm
    mkdir build
    cd build
    export PATH="/usr/local/opt/llvm/bin:$PATH"
    cmake .. -DCMAKE_CXX_COMPILER=`which clang++` -DCMAKE_C_COMPILER=`which clang` -DLINKER_NAME=ld -DUSE_STATIC_LIBRARIES=OFF
    make -j `sysctl -n hw.ncpu` clickhouse

Caveats
-------

If you intend to run clickhouse-server, make sure to increase system's maxfiles variable. See `MacOS.md <https://github.com/yandex/ClickHouse/blob/master/MacOS.md>`_ for more details.
