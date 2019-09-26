# RPM build specification file for Yandex ClickHouse DBMS
# Copyright (C) 2016 Red Soft LLC, 2017-2019 Altinity Ltd
# Licensed under the Apache License, Version 2.0 (the "License");

# All RPMs 'magic' is described here:
# https://fedoraproject.org/wiki/How_to_create_an_RPM_package
# rpm --showrc will show you all of the macros

# CentOS 7 makes dist as ".el7.centos", we'd like to have dist=".el7", in order to use packages for RHEL as well, without renaming
%if 0%{?rhel} != 0
%define dist .el%{rhel}
%endif

Summary: Yandex ClickHouse DBMS
Name: clickhouse
Version: 19.14.6.12
Release: 1%{?dist}
#Source0: %%{name}-%%{unmangled_version}.tar.gz
Source: ClickHouse-master-src.tar.gz
License: Apache License 2.0
Group: Applications/Databases
Url: https://clickhouse.yandex/

BuildRequires: make
BuildRequires: readline-devel, libicu-devel, libtool-ltdl-devel

%if 0%{?rhel} == 6 || 0%{?rhel} == 7
BuildRequires: devtoolset-8-gcc devtoolset-8-gcc-c++ cmake3
%else
BuildRequires: gcc gcc-c++ cmake
%endif

%description
ClickHouse is an open-source column-oriented database management
system that allows generating analytical data reports in real time.

#
# clickhouse-client
#
%package client
Summary: %{name} client binary
Requires: %{name}-common-static = %{version}-%{release}

%description client
This package provides clickhouse-client, clickhouse-local and clickhouse-benchmark.

#
# clickhouse-common-static
#
%package common-static

Summary: %{name} common static binaries

Requires: tzdata
# listing bundled (statically linked) packages is best practice in general, and requirement for official repos
# it gives possibility for admins and repo maintainers to check where potentially unsafe libs are used,

# TODO: review the list
Provides: bundled(arrow-devel)               = 0.11.0-592
Provides: bundled(boost-devel)               = 1.70
Provides: bundled(brotli-devel)              = 1.0.7-2
Provides: bundled(capnproto-devel)           = 0.7.0-a00ccd91
Provides: bundled(cctz-devel)                = 2.1-15-g4f9776a
Provides: bundled(cppkafka-devel)            = 0.2-96-g9b184d8
Provides: bundled(aklomp-base64-devel)       = 0.3.0-32-ga27c565
Provides: bundled(double-conversion-devel)   = 1.1-124-gcf2f0f3
Provides: bundled(cityhash)                  = 1.0.2
Provides: bundled(CRoaring-devel)            = 0.2.57
Provides: bundled(fastops-devel)             = 0.88752a5e03cf34639a4a37a4b41d8b463fffd2b5
Provides: bundled(googletest-devel)          = 1.8.0-389-gd175c8bf
Provides: bundled(h3-devel)                  = 3.4.2-5-g6cfd649
Provides: bundled(hyperscan-devel)           = 4.4.1-635-g3058c9c
Provides: bundled(jemalloc-devel)            = 5.1.0-97-gcd2931ad
Provides: bundled(libcxx-devel)              = 0.9807685d5
Provides: bundled(libcxxabi-devel)           = 0.d56efcc
Provides: bundled(libgsasl-devel)            = 0.3b8948a
Provides: bundled(libhdfs3-devel)            = 0.e2131aa
Provides: bundled(librdkafka-devel)          = 0.9.1-2350-g6160ec27
Provides: bundled(libunwind-devel)           = 9.0.0
Provides: bundled(libcpuid-devel)            = 0.4.0
Provides: bundled(libbtrie-devel)            = 0
Provides: bundled(libtcmalloc-devel)         = 2.5
Provides: bundled(libpcg-random-devel)       = 0
Provides: bundled(libmetrohash-devel)        = 1.1.3.690a521d
Provides: bundled(libfarmhash-devel)         = 1.1.afdb2e45
Provides: bundled(libdivide-devel)           = 0
Provides: bundled(murmurhash-devel)          = 0
Provides: bundled(pdqsort-devel)             = 0
Provides: bundled(libxml2-devel)             = 2.9.8
Provides: bundled(llvm-devel)                = 0.163def2
Provides: bundled(lz4-devel)                 = 1.8.3-244-g7a4e3b1
Provides: bundled(mariadb-connector-c-devel) = 3.0.2-318-gc6503d3
Provides: bundled(orc-devel)                 = 1.5.1
Provides: bundled(poco-devel)                = 1.6.0-release-2099-g6216cc01a
Provides: bundled(protobuf-cpp-devel)        = 3.6.1
Provides: bundled(rapidjson-devel)           = v1.1.0-487-g01950eb7
Provides: bundled(re2-devel)                 = 2018-01-01-2-g7cf8b88
Provides: bundled(simdjson-devel)            = v0.2.1-26-ge9be643
Provides: bundled(snappy-devel)              = 1.1.7-33-g3f194ac
Provides: bundled(libressl-devel)            = 2.6.4
Provides: bundled(thrift-devel)              = 0.10.0-675-g010ccf0a
Provides: bundled(unixodbc-devel)            = 2.3.6
Provides: bundled(zlib-ng-devel)             = 1.9.9
Provides: bundled(zstd-devel)                = 1.3.4


%description common-static
This package provides common files for both clickhouse server and client

#
# clickhouse-server
#
%package server
Summary: Server files for %{name}
Requires: %{name}-common-static = %{version}-%{release}
# Requires: initscripts # does it?
Obsoletes: %{name}-server-common < %{version}-%{release}
# Recommends & Suggests are not supported on CentOS 7
#Recommends: %%{name}-client = %%{version}-%%{release}
#Suggests: zookeeper

%description server
This package contains server files for ClickHouse DBMS.

#
# clickhouse-test
#
%package test
Summary: %{name} test suite
Requires: %{name}-server = %{version}-%{release}
Requires: python-lxml, python-requests, python2-pip, termcolor, perl, telnet

%description test
This package contains test suite for ClickHouse DBMS

##
## prep stage
##

%prep
%setup -qn ClickHouse

##
## build stage
##
%build
%if 0%{?rhel} == 6 || 0%{?rhel} == 7
. /opt/rh/devtoolset-8/enable
%endif

%cmake3 -DBUILD_SHARED_LIBS:BOOL=OFF .
make %{?_smp_mflags}


##
## install stage
##

%install

cd %{_builddir}/ClickHouse

rm -rf %{buildroot}
DAEMONS="clickhouse clickhouse-test clickhouse-compressor clickhouse-client clickhouse-server"
for daemon in $DAEMONS; do
    DESTDIR=%{buildroot} cmake3 -DCOMPONENT=$daemon -P cmake_install.cmake
done
cd ..

# Create folders structure to be distributed
# %%{buildroot} = rpmbuild/BUILDROOT/clickhouse-18.14.15-1.el7.x86_64
mkdir -p %{buildroot}/etc/clickhouse-server
mkdir -p %{buildroot}/etc/clickhouse-client/conf.d
mkdir -p %{buildroot}/etc/init.d
mkdir -p %{buildroot}/etc/cron.d
mkdir -p %{buildroot}/etc/security/limits.d

mkdir -p %{buildroot}/usr/bin
mkdir -p %{buildroot}/usr/share/clickhouse/bin
mkdir -p %{buildroot}/usr/share/clickhouse/headers

# Copy files from source into folders structure for distribution
# BUILDDIR = rpmbuild/BUILD
cp %{_builddir}/ClickHouse/debian/clickhouse-server.init   %{buildroot}/etc/init.d/clickhouse-server
cp %{_builddir}/ClickHouse/debian/clickhouse-server.cron.d %{buildroot}/etc/cron.d/clickhouse-server
cp %{_builddir}/ClickHouse/debian/clickhouse.limits        %{buildroot}/etc/security/limits.d/clickhouse.conf
cp %{_builddir}/ClickHouse/dbms/programs/server/config.xml %{buildroot}/etc/clickhouse-server/
cp %{_builddir}/ClickHouse/dbms/programs/server/users.xml  %{buildroot}/etc/clickhouse-server/

##
## clean stage
##

%clean
rm -rf %{buildroot}

##
## clickhouse-client package
##

%files client
%dir /etc/clickhouse-client/
%dir /etc/clickhouse-client/conf.d
/usr/bin/clickhouse-client
/usr/bin/clickhouse-local
/usr/bin/clickhouse-compressor
/usr/bin/clickhouse-benchmark
%config(noreplace) /etc/clickhouse-client/config.xml
/usr/bin/clickhouse-extract-from-config
/usr/bin/clickhouse-format
/usr/bin/clickhouse-obfuscator

##
## clickhouse-common-static package
##

%files common-static
/usr/bin/clickhouse
/usr/bin/clickhouse-odbc-bridge
%config(noreplace) /etc/security/limits.d/clickhouse.conf
# folder
/usr/share/clickhouse

##
## server package
##

%files server
%config(noreplace) /etc/clickhouse-server/config.xml
%config(noreplace) /etc/clickhouse-server/users.xml
/usr/bin/clickhouse-server
#/usr/bin/clickhouse-clang # https://github.com/ClickHouse/ClickHouse/pull/6646
#/usr/bin/clickhouse-lld
/usr/bin/clickhouse-copier
/usr/bin/clickhouse-report

# TODO
#/etc/systemd/system/clickhouse-server.service
%config /etc/init.d/clickhouse-server
%config /etc/cron.d/clickhouse-server
# folder
/usr/share/clickhouse
%config(noreplace) /etc/security/limits.d/clickhouse.conf
# append file that seems to be obsoleted
/usr/bin/clickhouse-format

%post server
if [ $1 = 1 ]; then
    /sbin/chkconfig --add clickhouse-server
#   if [ -x "/bin/systemctl" ] && [ -f /etc/systemd/system/clickhouse-server.service ]; then
#        /bin/systemctl daemon-reload
#        /bin/systemctl enable clickhouse-server
#    else
#        if [ -x "/etc/init.d/clickhouse-ser

fi

## TODO cleanup, review, use rpm ways
CLICKHOUSE_USER=clickhouse
CLICKHOUSE_GROUP=${CLICKHOUSE_USER}
CLICKHOUSE_DATADIR=/var/lib/clickhouse
CLICKHOUSE_LOGDIR=/var/log/clickhouse-server

##
##
##
function create_system_user()
{
    USER=$1
    GROUP=$2
    HOMEDIR=$3

    echo "Create user ${USER}.${GROUP} with datadir ${HOMEDIR}"

    # Make sure the administrative user exists
    if ! getent passwd ${USER} > /dev/null; then
        adduser \
            --system \
            --no-create-home \
            --home ${HOMEDIR} \
            --shell /sbin/nologin \
            --comment "Clickhouse server" \
            clickhouse > /dev/null
    fi

    # if the user was created manually, make sure the group is there as well
    if ! getent group ${GROUP} > /dev/null; then
        addgroup --system ${GROUP} > /dev/null
    fi

    # make sure user is in the correct group
    if ! id -Gn ${USER} | grep -qw ${USER}; then
        adduser ${USER} ${GROUP} > /dev/null
    fi

    # check validity of user and group
    if [ "`id -u ${USER}`" -eq 0 ]; then
        echo "The ${USER} system user must not have uid 0 (root). Please fix this and reinstall this package." >&2
            exit 1
    fi

    if [ "`id -g ${GROUP}`" -eq 0 ]; then
        echo "The ${USER} system user must not have root as primary group. Please fix this and reinstall this package." >&2
            exit 1
    fi
}


create_system_user $CLICKHOUSE_USER $CLICKHOUSE_GROUP $CLICKHOUSE_DATADIR

# Ensure required folders are in place
if [ ! -d ${CLICKHOUSE_DATADIR} ]; then
    mkdir -p ${CLICKHOUSE_DATADIR}
    chown ${CLICKHOUSE_USER}:${CLICKHOUSE_GROUP} ${CLICKHOUSE_DATADIR}
    chmod 700 ${CLICKHOUSE_DATADIR}
fi

if [ ! -d ${CLICKHOUSE_LOGDIR} ]; then
    mkdir -p ${CLICKHOUSE_LOGDIR}
    chown root:${CLICKHOUSE_GROUP} ${CLICKHOUSE_LOGDIR}
    # Allow everyone to read logs, root and clickhouse to read-write
    chmod 775 ${CLICKHOUSE_LOGDIR}
fi

# Clean old dynamic compilation results
if [ -d "${CLICKHOUSE_DATADIR}/build" ]; then
    rm -f ${CLICKHOUSE_DATADIR}/build/*.cpp ${CLICKHOUSE_DATADIR}/build/*.so ||:
fi

%preun server
if [ $1 = 0 ]; then
    /sbin/service clickhouse-server stop > /dev/null 2>&1
    /sbin/chkconfig --del clickhouse-server
fi

%postun server
if [ $1 -ge 1 ]; then
    /sbin/service clickhouse-server restart >/dev/null 2>&1
fi


##
## test package
##

%files test
/usr/bin/clickhouse-test
/usr/bin/clickhouse-test-server
/usr/bin/clickhouse-performance-test
# folder
/usr/share/clickhouse-test
%config(noreplace) /etc/clickhouse-client/client-test.xml
%config(noreplace) /etc/clickhouse-server/server-test.xml

##
##
##


## TODO:
# proper versions by macroses from packager
# service https://src.fedoraproject.org/rpms/postgresql/blob/master/f/postgresql.spec#_855
# better user creation
# check tests
# docs
# proper relative paths (https://docs.fedoraproject.org/en-US/packaging-guidelines/RPMMacros/)


%changelog
* Wed May 17 2017 Vadim Tkachenko
- Adjust to build 54231 release
* Fri Mar 31 2017 Nikolay Samofatov <nikolay.samofatov@red-soft.biz>
- fix issue #5 - error with creating clickhouse user
* Wed Mar 01 2017 Nikolay Samofatov <nikolay.samofatov@red-soft.biz>
- update for 1.1.54165
* Fri Feb 10 2017 Nikolay Samofatov <nikolay.samofatov@red-soft.biz>
- update for 1.1.54159
* Fri Jan 13 2017 Nikolay Samofatov <nikolay.samofatov@red-soft.biz>
- update for 1.1.54127
* Mon Nov 14 2016 Nikolay Samofatov <nikolay.samofatov@red-soft.biz>
- create spec file

