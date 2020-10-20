Attempt to build clickhouse-server image on the top of busybox instead of ubuntu. Non-tested.

How to use:
1) you need to get tgz packages - either by building them (see build_tgz.sh), either by downloading official build (see download_tgz.sh)
2) run prepare.sh to unpack tgz and prepare folder structure for future image
3) run `docker build . -t clickhouse-server:busybox`
