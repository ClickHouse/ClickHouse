FROM mcr.microsoft.com/dotnet/sdk:3.1

WORKDIR /client
COPY *.cs *.csproj /client/

ARG VERSION=3.1.0.379
RUN dotnet add package ClickHouse.Client -v ${VERSION}
RUN dotnet run
