# docker build .
# docker run -it --rm --network=host 14f23e59669c dotnet run --host localhost --port 8123 --user default --database default

FROM mcr.microsoft.com/dotnet/sdk:8.0

WORKDIR /client
COPY *.cs *.csproj /client/

ARG VERSION=4.1.0
# HOME=/tmp: redirect dotnet CLI home so nobody user can write global tool state.
# DOTNET_CLI_TELEMETRY_OPTOUT + DOTNET_NOLOGO: suppress network calls and banners.
ENV HOME=/tmp \
    DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    DOTNET_NOLOGO=1
RUN dotnet add package ClickHouse.Client -v ${VERSION} \
    && chmod -R 777 /client \
    && chmod -R 777 /tmp

# Run as non-root to satisfy CIS Docker Benchmark and Docker Scout policy.
# /client is 777 so dotnet run can write obj/ and bin/ build artifacts at test time.
# /tmp is 777 so nobody can write NuGet config/packages to $HOME (=/tmp).
USER nobody
