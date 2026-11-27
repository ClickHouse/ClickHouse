# docker build -t clickhouse/mysql_dotnet_client .
# Rebuild to enable SBOM/provenance attestations (see #97903, #98511)

FROM ubuntu:22.04

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

RUN apt-get update \
    && apt-get install -y software-properties-common build-essential dotnet-sdk-8.0 curl

# MySQL client app
WORKDIR /testapp
COPY testapp.csproj testapp.csproj
RUN dotnet restore
COPY Program.cs Program.cs

# PostgreSQL client app
WORKDIR /pg_testapp
COPY pg/testapp.csproj testapp.csproj
RUN dotnet restore
COPY pg/Program.cs Program.cs
