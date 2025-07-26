FROM ubuntu:18.04

RUN apt-get update \
    && apt-get install -y software-properties-common build-essential dotnet-sdk-8.0 curl

ARG ver=42.2.12
COPY Program.cs Program.cs
COPY testapp.csproj testapp.csproj
RUN dotnet add package MySql.Data
RUN dotnet add package CommandLineParser    

WORKDIR /testapp
