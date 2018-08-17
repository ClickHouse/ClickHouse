Allows to build ClickHouse in Docker.
This is useful if you have an old OS distribution and you don't want to build fresh gcc or clang from sources.

Usage:

Prepare image:
```
make image
```

Run build:
```
make build
```

Before run, ensure that your user has access to docker:  
To check, that you have access to Docker, run `docker ps`.  
If not, you must add this user to `docker` group: `sudo usermod -aG docker $USER` and relogin.  
(You must close all your sessions. For example, restart your computer.)

Build results are available in `build_docker` directory at top level of your working copy.  
It builds only binaries, not packages.

For example, run server:
```
cd $(git rev-parse --show-toplevel)/dbms/src/Server
$(git rev-parse --show-toplevel)/build_docker/dbms/src/Server/clickhouse server
```

Run client:
```
$(git rev-parse --show-toplevel)/build_docker/dbms/src/Server/clickhouse client
```
