FROM centos:7
# Usage:
# docker build -t yandex/centos-builder .
# docker run --rm --volume $(pwd)/../../..:/build/  --volume /tmp:/output -it yandex/centos-builder

# # set timezone
# RUN rm /etc/localtime
# RUN ln -s /usr/share/zoneinfo/Europe/Moscow /etc/localtime


RUN yum -y install epel-release centos-release-scl \
    # https://bugs.centos.org/view.php?id=14773
    && sed '/\[centos-sclo-sclo-source\]/,/gpgkey/d' -i /etc/yum.repos.d/CentOS-SCLo-scl.repo \
    && sed '/\[centos-sclo-rh-source\]/,/gpgkey/d' -i /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo \
    && yum -y install \
        rpmdevtools sudo rpmlint rpm-build redhat-rpm-config createrepo wget git python \
    && wget --no-check-certificate https://raw.githubusercontent.com/Kentzo/git-archive-all/master/git_archive_all.py \
          -O /usr/local/bin/git_archive_all \
    && chmod +x /usr/local/bin/git_archive_all \
    && adduser rpmbuilder \
    && echo 'ALL ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
    && yum -y install readline-devel libicu-devel libtool-ltdl-devel make devtoolset-8-gcc devtoolset-8-gcc-c++ cmake3 \
    && yum clean all

# non root build is
USER rpmbuilder

CMD rpmdev-setuptree \
    && ln -s /build/docker/packager/centos/clickhouse.spec ~/rpmbuild/SPECS/clickhouse.spec \
    # our first artefact is sources tarball
    && cd /build \
    && echo "Making source tarball..." \
    && git_archive_all --verbose --force-submodules --prefix=ClickHouse/ /output/ClickHouse-master-src.tar.gz \
    && ln -s /output/ClickHouse-master-src.tar.gz ~/rpmbuild/SOURCES/ClickHouse-master-src.tar.gz \
    # the resulting archive is quite fat (~250Mb) and can be reduced,
    # by cleaning some unneeded stuff from contrib
    #
    # potential candidates to remove from archive:
    # contrib/simdjson/dependencies/json/test       # 194M
    # contrib/simdjson/dependencies/json/benchmarks # 58Mb
    # contrib/llvm                                  # 110M
    # contrib/h3/tests/inputfiles                   # 41M
    # contrib/libcxx/test/std                       # 38Mb
    # contrib/libcxx/test/libcxx                    # 2.5Mb
    # contrib/double-conversion/test                # 19Mb
    # contrib/brotli/tests                          # 15Mb
    # contrib/libxml2/doc                           # 13M
    # contrib/libxml2/result                        # 15M
    # contrib/orc/examples                          # 23Mb
    # contrib/orc/site                              # 17M
    # git-archive-all claims that it should respect .gitattributes,
    # but it gives no results when i've tried to put those rules in top-level .gitattrbute
    && sudo yum-builddep -y ~/rpmbuild/SPECS/clickhouse.spec \
    && rpmbuild -v -ba ~/rpmbuild/SPECS/clickhouse.spec \
    # more artefacts
    && cp ~/rpmbuild/RPMS/* /output/ \
    && cp ~/rpmbuild/SRPMS/* /output/

# Install ClickHouse from local RPMs
# yum localinstall -y /clickhouse/*.rpm

#rpmdev-bumpspec --comment="Automatic nightly release" --userstring="Jenkins <ring@lists.savoirfairelinux.net>" ring.spec
# install build deps
#dnf builddep -y ring.spec || echo "ignoring dnf builddep failure"