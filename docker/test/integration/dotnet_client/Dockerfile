# docker build .
# docker run -it --rm --network=host 14f23e59669c dotnet run --host localhost --port 8123 --user default --database default

FROM mcr.microsoft.com/dotnet/sdk:3.1

WORKDIR /client
COPY *.cs *.csproj /client/

ARG VERSION=4.1.0
RUN dotnet add package ClickHouse.Client -v ${VERSION}
