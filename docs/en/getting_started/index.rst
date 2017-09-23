Getting started
===============

System requirements
-------------------

This is not a cross-platform system. It requires Linux Ubuntu Precise (12.04) or newer, x86_64 architecture with SSE 4.2 instruction set.
To test for SSE 4.2 support, do:

.. code-block:: text

    grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"

We recommend using Ubuntu Trusty or Ubuntu Xenial or Ubuntu Precise.
The terminal must use UTF-8 encoding (the default in Ubuntu).

Installation
------------

For testing and development, the system can be installed on a single server or on a desktop computer.

Installing from packages
~~~~~~~~~~~~~~~~~~~~~~~~

In `/etc/apt/sources.list` (or in a separate `/etc/apt/sources.list.d/clickhouse.list` file), add the repository:

.. code-block:: text

    deb http://repo.yandex.ru/clickhouse/trusty stable main

For other Ubuntu versions, replace `trusty` to `xenial` or `precise`.
If you want to use the most fresh testing version of ClickHouse, replace `stable` to `testing`.

Then run:

.. code-block:: bash

    sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4    # optional
    sudo apt-get update
    sudo apt-get install clickhouse-client clickhouse-server

You can also download and install packages manually from here:
http://repo.yandex.ru/clickhouse/trusty/pool/main/c/clickhouse/,
http://repo.yandex.ru/clickhouse/xenial/pool/main/c/clickhouse/,
http://repo.yandex.ru/clickhouse/precise/pool/main/c/clickhouse/.

ClickHouse contains access restriction settings. They are located in the 'users.xml' file (next to 'config.xml').
By default, access is allowed from everywhere for the default user without a password. See 'user/default/networks'. For more information, see the section "Configuration files".

Installing from source
~~~~~~~~~~~~~~~~~~~~~~
To build, follow the instructions in build.md (for Linux) or in build_osx.md (for Mac OS X).

You can compile packages and install them. You can also use programs without installing packages.

.. code-block:: text

    Client: dbms/src/Client/
    Server: dbms/src/Server/

For the server, create a directory with data, such as:

.. code-block:: text

    /opt/clickhouse/data/default/
    /opt/clickhouse/metadata/default/

(Configured in the server config.)
Run 'chown' for the desired user.

Note the path to logs in the server config (src/dbms/src/Server/config.xml).

Other methods of installation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The Docker image is located here: https://hub.docker.com/r/yandex/clickhouse-server/

There are RPM packages for CentOS, RHEL: https://github.com/Altinity/clickhouse-rpm-install

There is Gentoo overlay located here: https://github.com/kmeaw/clickhouse-overlay


Launch
------

To start the server (as a daemon), run:

.. code-block:: bash

    sudo service clickhouse-server start

View the logs in the directory `/var/log/clickhouse-server/`

If the server doesn't start, check the configurations in the file `/etc/clickhouse-server/config.xml`

You can also launch the server from the console:

.. code-block:: bash

    clickhouse-server --config-file=/etc/clickhouse-server/config.xml

In this case, the log will be printed to the console, which is convenient during development. If the configuration file is in the current directory, you don't need to specify the '--config-file' parameter. By default, it uses './config.xml'.

You can use the command-line client to connect to the server:

.. code-block:: bash

    clickhouse-client

The default parameters indicate connecting with localhost:9000 on behalf of the user 'default' without a password.
The client can be used for connecting to a remote server. For example:

.. code-block:: bash

    clickhouse-client --host=example.com

For more information, see the section "Command-line client".

Checking the system:

.. code-block:: bash

    milovidov@hostname:~/work/metrica/src/dbms/src/Client$ ./clickhouse-client
    ClickHouse client version 0.0.18749.
    Connecting to localhost:9000.
    Connected to ClickHouse server version 0.0.18749.

    :) SELECT 1

    SELECT 1

    ┌─1─┐
    │ 1 │
    └───┘

    1 rows in set. Elapsed: 0.003 sec.

    :)

**Congratulations, it works!**

For further experiments you can try playing with one of the following example datasets:

.. toctree::
    :glob:

    example_datasets/*

