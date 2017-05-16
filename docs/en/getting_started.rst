Getting started
=============
    
System requirements
-----------------

This is not a cross-platform system. It requires Linux Ubuntu Precise (12.04) or newer, x86_64 architecture with SSE 4.2 instruction set.
To test for SSE 4.2 support, do:
::
    grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"

We recommend using Ubuntu Trusty or Ubuntu Xenial or Ubuntu Precise.
The terminal must use UTF-8 encoding (the default in Ubuntu).

Installation
-----------------

For testing and development, the system can be installed on a single server or on a desktop computer.

Installing from packages
~~~~~~~~~~~~~~~~~~~~

In `/etc/apt/sources.list` (or in a separate `/etc/apt/sources.list.d/clickhouse.list` file), add the repository: 
::
    deb http://repo.yandex.ru/clickhouse/trusty stable main

For other Ubuntu versions, replace `trusty` to `xenial` or `precise`.

Then run:
::
    sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4    # optional
    sudo apt-get update
    sudo apt-get install clickhouse-client clickhouse-server-common
    
You can also download and install packages manually from here:
http://repo.yandex.ru/clickhouse/trusty/pool/main/c/clickhouse/,
http://repo.yandex.ru/clickhouse/xenial/pool/main/c/clickhouse/,
http://repo.yandex.ru/clickhouse/precise/pool/main/c/clickhouse/.

ClickHouse contains access restriction settings. They are located in the 'users.xml' file (next to 'config.xml').
By default, access is allowed from everywhere for the default user without a password. See 'user/default/networks'. For more information, see the section "Configuration files".

Installing from source
~~~~~~~~~~~~~~~~~~~~~~~
To build, follow the instructions in build.md (for Linux) or in build_osx.md (for Mac OS X).

You can compile packages and install them. You can also use programs without installing packages.
::
    Client: dbms/src/Client/
    Server: dbms/src/Server/

For the server, create a catalog with data, such as:
::
    /opt/clickhouse/data/default/
    /opt/clickhouse/metadata/default/
    
(Configured in the server config.)
Run 'chown' for the desired user.

Note the path to logs in the server config (src/dbms/src/Server/config.xml).

Other methods of installation
~~~~~~~~~~~~~~~~~~~~~~~
The Docker image is located here: https://hub.docker.com/r/yandex/clickhouse-server/

There is Gentoo overlay located here: https://github.com/kmeaw/clickhouse-overlay


Launch
-------

To start the server (as a daemon), run:
::
    sudo service clickhouse-server start
    
View the logs in the catalog `/var/log/clickhouse-server/`

If the server doesn't start, check the configurations in the file `/etc/clickhouse-server/config.xml`

You can also launch the server from the console:
::
    clickhouse-server --config-file=/etc/clickhouse-server/config.xml
    
In this case, the log will be printed to the console, which is convenient during development. If the configuration file is in the current directory, you don't need to specify the '--config-file' parameter. By default, it uses './config.xml'.

You can use the command-line client to connect to the server:
::
    clickhouse-client

The default parameters indicate connecting with localhost:9000 on behalf of the user 'default' without a password.
The client can be used for connecting to a remote server. For example:
::
    clickhouse-client --host=example.com
    
For more information, see the section "Command-line client".

Checking the system:
::
    milovidov@milovidov-Latitude-E6320:~/work/metrica/src/dbms/src/Client$ ./clickhouse-client
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

Congratulations, it works!

Test data
---------------
If you are Yandex employee, you can use Yandex.Metrica test data to explore the system's capabilities. You can find instructions for using the test data here.

Otherwise, you could use one of available public datasets, described here.


If you have questions
---------------------
If you are Yandex employee, use internal ClickHouse maillist.
You can subscribe to this list to get announcements, information on new developments, and questions that other users have.

Otherwise, you could ask questions on Stack Overflow; discuss in Google Groups; or send private message to developers to address clickhouse-feedback@yandex-team.com.
