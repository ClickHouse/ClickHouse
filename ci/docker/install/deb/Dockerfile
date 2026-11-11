FROM ubuntu:22.04

# The Dockerfile is nicely borrowed from
# https://github.com/lionelnicolas/docker-ubuntu-systemd/blob/83aa3249146f5df264fe45353f79fc76eb1e42d7/Dockerfile

ENV \
  DEBIAN_FRONTEND=noninteractive \
  LANG=C.UTF-8 \
  container=docker \
  init=/lib/systemd/systemd

# Redirect apt to the mirror passed by CI (canonical default; see ci/praktika/docker.py).
# Rewrite archive.ubuntu.com (amd64) and ports.ubuntu.com (arm64) across sources.list
# and deb822 *.sources so arm64 builds avoid the flaky ports.ubuntu.com.
ARG apt_archive="http://archive.ubuntu.com"
ARG apt_ports_archive="http://ports.ubuntu.com"
RUN find /etc/apt -type f \( -name '*.list' -o -name '*.sources' \) -print0 | xargs -0 -r sed -i \
        -e "s|http://archive.ubuntu.com|$apt_archive|g" \
        -e "s|https://archive.ubuntu.com|$apt_archive|g" \
        -e "s|http://ports.ubuntu.com|$apt_ports_archive|g" \
        -e "s|https://ports.ubuntu.com|$apt_ports_archive|g"

# install systemd packages
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    sudo \
    systemd \
    \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# configure systemd
# remove systemd 'wants' triggers
# remove everything except tmpfiles setup in sysinit target
# remove UTMP updater service
# disable /tmp mount
# fix missing BPF firewall support warning
# just for cosmetics, fix "not-found" entries while using "systemctl --all"
RUN \
  find \
    /etc/systemd/system/*.wants/* \
    /lib/systemd/system/multi-user.target.wants/* \
    /lib/systemd/system/sockets.target.wants/*initctl* \
    ! -type d \
    -delete && \
  find \
    /lib/systemd/system/sysinit.target.wants \
    ! -type d \
    ! -name '*systemd-tmpfiles-setup*' \
    -delete && \
  find \
    /lib/systemd \
    -name systemd-update-utmp-runlevel.service \
    -delete && \
  rm -vf /usr/share/systemd/tmp.mount && \
  sed -ri '/^IPAddressDeny/d' /lib/systemd/system/systemd-journald.service && \
  for MATCH in \
    plymouth-start.service \
    plymouth-quit-wait.service \
    syslog.socket \
    syslog.service \
    display-manager.service \
    systemd-sysusers.service \
    tmp.mount \
    systemd-udevd.service \
    ; do \
      grep -rn --binary-files=without-match  ${MATCH} /lib/systemd/ | cut -d: -f1 | xargs sed -ri 's/(.*=.*)'${MATCH}'(.*)/\1\2/'; \
  done && \
  systemctl disable ondemand.service && \
  systemctl set-default multi-user.target

VOLUME ["/run", "/run/lock"]

STOPSIGNAL SIGRTMIN+3

ENTRYPOINT ["/lib/systemd/systemd"]
