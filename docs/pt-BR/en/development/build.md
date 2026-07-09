---
description: 'Guia passo a passo para compilação o ClickHouse a partir do código-fonte em sistemas Linux'
sidebar_label: 'Compilação no Linux'
sidebar_position: 10
slug: /development/build
title: 'Como compilação o ClickHouse no Linux'
doc_type: 'guide'
---

:::info Este guia de compilação é destinado a colaboradores que modificam o próprio ClickHouse.
Se você não estiver alterando o código-fonte do ClickHouse, poderá instalar uma versão pré-compilada do ClickHouse conforme descrito no [Quick Start](https://clickhouse.com/docs/get-started/quick-start).
:::

O ClickHouse pode ser compilado nas seguintes plataformas:

* x86&#95;64
* AArch64
* PowerPC 64 LE (experimental)
* s390/x (experimental)
* RISC-V 64 (experimental)

<div id="assumptions">
  ## Premissas
</div>

O tutorial a seguir é baseado em Ubuntu Linux, mas também deve funcionar em qualquer outra distribuição Linux com as devidas adaptações.
A versão mínima recomendada do Ubuntu para desenvolvimento é a 24.04 LTS.

O tutorial pressupõe que você já tenha o repositório do ClickHouse e todos os submódulos clonados localmente.

<div id="install-prerequisites">
  ## Instale os pré-requisitos
</div>

Primeiro, consulte a [documentação geral de pré-requisitos](developer-instruction.md).

O ClickHouse usa CMake e Ninja para compilação.

Opcionalmente, você pode instalar o ccache para que a compilação reutilize arquivos objeto já compilados.

```bash
sudo apt-get update
sudo apt-get install build-essential git cmake ccache python3 ninja-build nasm yasm gawk lsb-release wget software-properties-common gnupg
```

<div id="install-the-clang-compiler">
  ## Instale o compilador Clang
</div>

Para instalar o Clang no Ubuntu/Debian, use o script de instalação automática do LLVM disponível [aqui](https://apt.llvm.org/).

```bash
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 21
```

Para outras distribuições Linux, verifique se é possível instalar algum dos [pacotes pré-compilados](https://releases.llvm.org/download.html) do LLVM.

Desde fevereiro de 2026, é necessário usar o Clang 21 ou superior.
GCC e outros compiladores não são suportados.

<div id="install-the-rust-compiler-optional">
  ## Instale o compilador Rust (opcional)
</div>

:::note
Rust é uma dependência opcional do ClickHouse.
Se o Rust não estiver instalado, alguns recursos do ClickHouse serão omitidos da compilação.
:::

Primeiro, siga as etapas da [documentação oficial do Rust](https://www.rust-lang.org/tools/install) para instalar o `rustup`.

Assim como com as dependências de C++, o ClickHouse usa vendoring para controlar exatamente o que é instalado e evitar depender de serviços de terceiros (como o registro `crates.io`).

Embora, no modo release, qualquer versão moderna do toolchain do rustup deva funcionar com essas dependências, se você pretende habilitar sanitizers, deve usar uma versão que corresponda exatamente ao mesmo `std` usado na CI (para o qual fazemos vendoring dos crates):

```bash
rustup toolchain install nightly-2026-03-22
rustup default nightly-2026-03-22
rustup component add rust-src
```

<div id="build-clickhouse">
  ## Compilação do ClickHouse
</div>

Recomendamos criar um diretório separado chamado `build` dentro de `ClickHouse` para conter todos os artefatos da compilação:

```sh
mkdir build
cd build
```

Você pode ter vários diretórios diferentes (por exemplo, `build_release`, `build_debug` etc.) para diferentes tipos de compilação.

Opcional: se você tiver várias versões do compilador instaladas, também poderá especificar exatamente qual compilador usar.

```sh
export CC=clang-21
export CXX=clang++-21
```

Para fins de desenvolvimento, recomenda-se usar compilações de depuração.
Em comparação com as compilações de lançamento, elas têm um nível menor de otimização do compilador (`-O`), o que proporciona uma experiência de depuração melhor.
Além disso, exceções internas do tipo `LOGICAL_ERROR` encerram o programa imediatamente, em vez de falhar de forma controlada.

```sh
cmake -D CMAKE_BUILD_TYPE=Debug ..
```

:::note
Se você quiser usar um depurador como o gdb, adicione `-D DEBUG_O_LEVEL="0"` ao comando acima para remover todas as otimizações do compilador, que podem atrapalhar a capacidade do gdb de visualizar/acessar variáveis.
:::

Execute `ninja` para fazer a compilação:

```sh
ninja clickhouse
```

Se quiser compilar todos os binários (utilitários e testes), execute `ninja` sem parâmetros:

```sh
ninja
```

Você pode controlar o número de processos paralelos de compilação usando o parâmetro `-j`:

```sh
ninja -j 1 clickhouse
```

:::note
`clickhouse-server`, `clickhouse-client` e binários semelhantes são links simbólicos no diretório `programs/` que apontam para o executável `clickhouse` após a conclusão da compilação.

:::tip
O CMake oferece atalhos para os comandos acima:

```sh
cmake -S . -B build  # configure build, run from repository top-level directory
cmake --build build  # compile
```

:::

<div id="running-the-clickhouse-executable">
  ## Executando o executável do ClickHouse
</div>

Após a compilação ser concluída com sucesso, você encontrará o executável em `ClickHouse/<build_dir>/programs/`:

O servidor ClickHouse tenta encontrar um arquivo de configuração `config.xml` no diretório atual.
Como alternativa, você pode especificar um arquivo de configuração na linha de comando usando `-C`.

Para se conectar ao servidor ClickHouse com `clickhouse-client`, abra outro terminal, navegue até `ClickHouse/build/programs/` e execute `./clickhouse client`.

Se você receber a mensagem `Connection refused` no macOS ou FreeBSD, tente especificar o host 127.0.0.1:

```bash
clickhouse client --host 127.0.0.1
```

<div id="advanced-options">
  ## Opções avançadas
</div>

<div id="minimal-build">
  ### Compilação mínima
</div>

Se você não precisa dos recursos fornecidos por bibliotecas de terceiros, pode acelerar ainda mais a compilação:

```sh
cmake -DENABLE_LIBRARIES=OFF
```

Em caso de problemas, você estará por sua conta ...

O Rust requer uma conexão com a internet. Para desativar o suporte ao Rust:

```sh
cmake -DENABLE_RUST=OFF
```

<div id="running-the-clickhouse-executable-1">
  ### Executando o executável do ClickHouse
</div>

Você pode substituir a versão em produção do binário do ClickHouse instalada no seu sistema pelo binário compilado do ClickHouse.
Para fazer isso, instale o ClickHouse na sua máquina seguindo as instruções do site oficial.
Em seguida, execute:

```bash
sudo service clickhouse-server stop
sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
sudo service clickhouse-server start
```

Observe que `clickhouse-client`, `clickhouse-server` e outros são links simbólicos para o binário `clickhouse` compartilhado.

Você também pode executar seu binário personalizado do ClickHouse com o arquivo de configuração do pacote ClickHouse instalado no seu sistema:

```bash
sudo service clickhouse-server stop
sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

<div id="building-on-any-linux">
  ### Compilando em qualquer distribuição Linux
</div>

Instale os pré-requisitos no OpenSUSE Tumbleweed:

```bash
sudo zypper install git cmake ninja clang-c++ python lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

Instale os pré-requisitos no Fedora Rawhide:

```bash
sudo yum update
sudo yum --nogpg install git cmake make clang python3 ccache lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

<div id="building-in-docker">
  ### Compilando no Docker
</div>

Você pode executar qualquer compilação localmente em um ambiente semelhante ao de CI usando:

```bash
python -m ci.praktika run "BUILD_JOB_NAME"
```

em que BUILD&#95;JOB&#95;NAME é o nome do job, conforme mostrado no relatório de CI, por exemplo, &quot;Build (arm&#95;release)&quot;, &quot;Build (amd&#95;debug)&quot;

Este comando baixa a imagem Docker apropriada `clickhouse/binary-builder` com todas as dependências necessárias
e executa nela o script de compilação: `./ci/jobs/build_clickhouse.py`

A saída da compilação será colocada em `./ci/tmp/`.

Funciona em arquiteturas AMD e ARM e não requer dependências adicionais além de Python com o módulo `requests` disponível e Docker.