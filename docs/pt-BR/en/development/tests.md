---
description: 'Guia para testar o ClickHouse e executar a suíte de testes'
sidebar_label: 'Testes'
sidebar_position: 40
slug: /development/tests
title: 'Testes do ClickHouse'
doc_type: 'guide'
---

<div id="test-types">
  ## Tipos de teste
</div>

Há os seguintes testes no ClickHouse:

* [Testes funcionais](#functional-tests) - um conjunto de consultas e scripts que inclui os seguintes subconjuntos sobrepostos
  * [Teste rápido](#running-fast-tests) - o subconjunto mínimo
  * [Testes sem estado](#running-stateless-tests) que não exigem o preenchimento de bancos de dados com dados
  * Testes sequenciais que não podem ser executados em paralelo
* [Testes de integração](#integration-tests), executados pelo `pytest` em um cluster
* [Testes unitários](#unit-tests)
* [Testes de desempenho](#performance-tests)
* [Testes de compilação](#build-tests)
* [Sanitizers](#sanitizers)
* [Fuzzers](#fuzzing)
  e alguns outros; veja as seções abaixo.

<div id="functional-tests">
  ## Testes funcionais
</div>

Os testes funcionais são os mais simples e convenientes de usar.
A maioria das funcionalidades do ClickHouse pode ser testada com testes funcionais, e eles são obrigatórios para toda alteração no código do ClickHouse que possa ser testada dessa forma.

Cada teste funcional envia uma ou mais consultas ao servidor ClickHouse em execução e compara o resultado com a referência.

Os testes estão localizados no diretório `./tests/queries`.

Cada teste pode ser de um de dois tipos: `.sql` e `.sh`.

* Um teste `.sql` é um script SQL simples enviado por pipe para o `clickhouse-client`.
* Um teste `.sh` é um script executado diretamente.

Em geral, os testes SQL são preferíveis aos testes `.sh`.
Você deve usar testes `.sh` somente quando precisar testar alguma funcionalidade que não possa ser exercitada com SQL puro, como enviar dados de entrada por pipe para o `clickhouse-client` ou testar o `clickhouse-local`.

:::note
Um erro comum ao testar os tipos de dados `DateTime` e `DateTime64` é presumir que o servidor usa um fuso horário específico (por exemplo, &quot;UTC&quot;). Não é o caso; os fusos horários nas execuções de teste de CI
são deliberadamente aleatorizados. A solução alternativa mais simples é especificar explicitamente o fuso horário dos valores de teste, por exemplo, `toDateTime64(val, 3, 'Europe/Amsterdam')`.
:::

<div id="running-a-test-locally">
  ### Executando um teste localmente
</div>

Inicie o servidor ClickHouse localmente, ouvindo na porta padrão (9000).
Para executar, por exemplo, o teste `01428_hash_set_nan_key`, acesse a pasta do repositório e execute o seguinte comando:

```sh
PATH=<path to clickhouse-client>:$PATH tests/clickhouse-test 01428_hash_set_nan_key
```

Os resultados do teste (`stderr` e `stdout`) são gravados nos arquivos `01428_hash_set_nan_key.[stderr|stdout]`, que ficam ao lado do próprio teste (para `queries/0_stateless/foo.sql`, a saída estará em `queries/0_stateless/foo.stdout`).

Consulte `tests/clickhouse-test --help` para ver todas as opções de `clickhouse-test`.
Você pode executar todos os testes ou um subconjunto deles, informando um filtro para os nomes dos testes: `./clickhouse-test substring`.
Também há opções para executar os testes em paralelo ou em ordem aleatória.

<div id="running-tests-on-macos">
  #### Executando testes no macOS (Darwin)
</div>

Muitos testes funcionais executam utilitários GNU de linha de comando no shell (`timeout`, `head`, `sed`, `grep`, `date` etc.). O macOS inclui as variantes BSD dessas ferramentas, cujo comportamento e cujas opções são diferentes (por exemplo, o `head` do BSD rejeita `head -c 1G`, o `ps` do BSD não tem as opções longas `--` e não há `timeout`). Executar os testes com as ferramentas BSD gera falhas espúrias.

Os runners de CI do macOS instalam as ferramentas GNU via Homebrew e as colocam antes das ferramentas BSD no `PATH`. Reproduza isso localmente:

```sh
brew install coreutils gnu-sed grep
export PATH="$(brew --prefix)/opt/coreutils/libexec/gnubin:$(brew --prefix)/opt/gnu-sed/libexec/gnubin:$(brew --prefix)/opt/grep/libexec/gnubin:$PATH"
```

`coreutils` fornece GNU `timeout`, `head`, `date` e afins; `gnu-sed` e `grep` fornecem GNU `sed` e `grep`. Depois disso, `which timeout head sed grep` deve apontar para os caminhos do `gnubin`.

<div id="running-fast-tests">
  ### Executando testes rápidos
</div>

Talvez você precise de uma máquina razoavelmente potente para executar um subconjunto de testes (chamado &quot;teste rápido&quot;). O procedimento a seguir funciona em uma instância Ubuntu amd64 `t3.2xlarge` na AWS com 100 GB de armazenamento.

1. Instale os pré-requisitos e faça login novamente.

```sh
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker "$USER"
```

2. Baixe o código-fonte.

```sh
git clone --single-branch https://github.com/ClickHouse/ClickHouse
cd ClickHouse
```

3. Faça a compilação do código e execute os &quot;testes rápidos&quot;.

```sh
python -m ci.praktika run fast
```

O resultado deve ser

```sh
Failed: 0, Passed: 7394, Skipped: 1795
```

Se você deixar o processo sem supervisão, poderá usar `nohup` ou `disown` para que ele continue em execução mesmo que a conexão `ssh` seja perdida.

<div id="running-stateless-tests">
  ### Executando testes sem estado
</div>

Talvez você precise de uma máquina razoavelmente potente para executar testes sem estado. A configuração abaixo funciona em uma instância Ubuntu amd64 `m7i.8xlarge` da AWS com 200 GB de armazenamento.

1. Instale os pré-requisitos e faça login novamente.

```sh
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker "$USER"
sudo tee /etc/docker/daemon.json <<'EOF'
{
  "ipv6": true,
  "ip6tables": true
}
EOF
sudo systemctl restart docker
```

2. Baixe o código-fonte.

```sh
git clone --single-branch https://github.com/ClickHouse/ClickHouse
cd ClickHouse
```

3. Faça a compilação do código.

```sh
python -m ci.praktika run build_debug
cp ci/tmp/build/programs/clickhouse ci/tmp
```

4. Execute testes sem estado que podem ser executados em paralelo.

```sh
python -m ci.praktika run functional
```

O resultado deve ser

```sh
Failed: 0, Passed: 8497, Skipped: 103
```

Observação. As execuções de `python -m ci.praktika run` executam um job específico de integração contínua; leia mais sobre a CI do ClickHouse [aqui](continuous-integration.md#running-stateless-tests).

<div id="adding-a-new-test">
  ### Adicionando um novo teste
</div>

Para adicionar um novo teste, primeiro crie um arquivo `.sql` ou `.sh` no diretório `queries/0_stateless`.
Em seguida, gere o arquivo `.reference` correspondente usando `clickhouse-client < 12345_test.sql > 12345_test.reference` ou `./12345_test.sh > ./12345_test.reference`.

Os testes devem apenas criar, remover, consultar etc. tabelas no banco de dados `test`, que é criado automaticamente com antecedência.
É permitido usar tabelas temporárias.

Para configurar localmente o mesmo ambiente da CI, instale as configurações de teste (elas usarão uma implementação simulada do Zookeeper e ajustarão algumas configurações)

```sh
cd <repository>/tests/config
sudo ./install.sh
```

:::note
Os testes devem ser

* mínimos: criar apenas as tabelas, colunas e a complexidade estritamente necessárias,
* rápidos: não levar mais do que alguns segundos (de preferência, menos de um segundo),
* corretos e determinísticos: falhar se, e somente se, a funcionalidade em teste não estiver funcionando,
* isolados/sem estado: não depender do ambiente nem de temporização,
* exaustivos: cobrir casos extremos, como zeros, nulls, conjuntos vazios e exceções (testes negativos; para isso, use a sintaxe `-- { serverError xyz }` e `-- { clientError xyz }`),
* limpar as tabelas ao final do teste (caso sobrem resíduos),
* garantir que os outros testes não verifiquem a mesma coisa (ou seja, use `grep` primeiro).
  :::

<div id="templated-tests-with-jinja">
  ### Testes com templates em Jinja
</div>

Um teste `.sql` pode ser escrito como um template [Jinja2](https://jinja.palletsprojects.com/) adicionando o sufixo `.j2` ao nome do arquivo, de modo que `foo.sql` passe a ser `foo.sql.j2`. Antes de executar o teste, o `clickhouse-test` renderiza o template como um script `.sql` comum e executa o resultado.

Isso é útil quando um teste repete a mesma consulta com pequenas variações: um loop gera as consultas a partir de um template conciso, em vez de escrever cada uma manualmente. As construções mais usadas são:

* `{% for ... %} ... {% endfor %}` para repetir um bloco,
* `{{ expression }}` para inserir um valor na saída,
* `-%}` e `{%-` para remover espaços em branco adjacentes, de modo que o script gerado permaneça limpo.

Por exemplo, este template:

```sql
{% for type in ['UInt8', 'UInt16', 'UInt32'] -%}
SELECT toTypeName(0::{{ type }});
{% endfor -%}
```

resulta em:

```sql
SELECT toTypeName(0::UInt8);
SELECT toTypeName(0::UInt16);
SELECT toTypeName(0::UInt32);
```

A saída esperada pode ser fornecida como um arquivo `<name>.reference` simples, contendo os resultados totalmente expandidos, ou como um template `<name>.reference.j2`, que o `clickhouse-test` processa da mesma forma antes de comparar. Use a forma com template quando a saída esperada também seguir um padrão repetitivo. Para mais exemplos, veja os arquivos `*.sql.j2` existentes em `tests/queries/0_stateless/`.

<div id="restricting-test-runs">
  ### Restringindo execuções de teste
</div>

Um teste pode ter zero ou mais *tags* que especificam restrições sobre em quais contextos o teste é executado no CI.

Para testes `.sql`, as tags são colocadas na primeira linha como um comentário SQL:

```sql
-- Tags: no-fasttest, no-replicated-database
-- no-fasttest: <provide_a_reason_for_the_tag_here>
-- no-replicated-database: <provide_a_reason_here>

SELECT 1
```

Nos testes `.sh`, as tags são escritas como comentário na segunda linha:

```bash
#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: <provide_a_reason_for_the_tag_here>
# - no-replicated-database: <provide_a_reason_here>
```

Lista de tags disponíveis:

| Tag name                       | O que faz                                                                         | Usage example                                                                                            |
| ------------------------------ | --------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `disabled`                     | O teste não é executado                                                           |                                                                                                          |
| `long`                         | O tempo de execução do teste é estendido de 1 para 10 minutos                     |                                                                                                          |
| `deadlock`                     | O teste é executado em loop por um longo período                                  |                                                                                                          |
| `race`                         | O mesmo que `deadlock`. Prefira `deadlock`                                        |                                                                                                          |
| `shard`                        | O servidor deve escutar em `127.0.0.*`                                            |                                                                                                          |
| `distributed`                  | O mesmo que `shard`. Prefira `shard`                                              |                                                                                                          |
| `global`                       | O mesmo que `shard`. Prefira `shard`                                              |                                                                                                          |
| `zookeeper`                    | O teste requer ZooKeeper ou ClickHouse Keeper para ser executado                  | O teste usa `ReplicatedMergeTree`                                                                        |
| `replica`                      | O mesmo que `zookeeper`. Prefira `zookeeper`                                      |                                                                                                          |
| `no-fasttest`                  | O teste não é executado em [teste rápido](#test-types)                            | O teste usa o table engine `MySQL`, que é desativado em teste rápido                                     |
| `fasttest-only`                | O teste é executado apenas em [teste rápido](#test-types)                         |                                                                                                          |
| `no-[asan, tsan, msan, ubsan]` | Desabilita testes em builds com [sanitizers](#sanitizers)                         | O teste é executado com QEMU, que não funciona com sanitizers                                            |
| `no-replicated-database`       | Desabilita o teste quando o banco de dados padrão usa `ReplicatedDatabaseEngine`  |                                                                                                          |
| `no-ordinary-database`         | Desabilita o teste quando o database engine do banco de dados padrão é `Ordinary` |                                                                                                          |
| `no-parallel`                  | Desabilita a execução de outros testes em paralelo com este                       | O teste lê tabelas `system` e os invariantes podem ser violados                                          |
| `no-parallel-replicas`         | Desabilita o teste quando parallel replicas estão habilitadas                     |                                                                                                          |
| `no-debug`                     | Desabilita testes em builds Debug                                                 |                                                                                                          |
| `no-release`                   | Desabilita testes em builds Release                                               |                                                                                                          |
| `no-darwin`                    | Desabilita o teste no macOS (Darwin)                                              | O teste depende de recursos específicos do Linux, como consultas distribuídas, `procfs` ou servidor HTTP |

As opções a seguir também são compatíveis: `no-polymorphic-parts`, `no-random-settings`, `no-random-merge-tree-settings`, `no-backward-compatibility-check`, `no-cpu-x86_64`, `no-cpu-aarch64`, `no-cpu-ppc64le`, `no-s3-storage`.

Além das configurações acima, você também pode usar flags `USE_*` de `system.build_options` para indicar o uso de funcionalidades específicas do ClickHouse.
Por exemplo, se o seu teste usa uma tabela MySQL, você deve adicionar a tag `use-mysql`.

<div id="specifying-limits-for-random-settings">
  ### Especificando limites para configurações aleatórias
</div>

Um teste pode especificar os valores mínimo e máximo permitidos para configurações que podem ser aleatorizadas durante a execução do teste.

Nos testes `.sh`, os limites são escritos como um comentário na linha ao lado das tags ou na segunda linha, caso nenhuma tag seja especificada:

```bash
#!/usr/bin/env bash
# Tags: no-fasttest
# Random settings limits: max_block_size=(1000, 10000); index_granularity=(100, None)
```

Para testes `.sql`, as tags são colocadas em um comentário SQL na linha ao lado de tags ou na primeira linha:

```sql
-- Tags: no-fasttest
-- Random settings limits: max_block_size=(1000, 10000); index_granularity=(100, None)
SELECT 1
```

Se você precisar especificar apenas um limite, pode usar `None` para o outro.

<div id="choosing-the-test-name">
  ### Escolhendo o nome do teste
</div>

O nome do teste começa com um prefixo de cinco díitos seguido de um nome descritivo, como `00422_hash_function_constexpr.sql`.
Para escolher o prefixo, encontre o maior prefixo já presente no diretório e some um a ele.

```sh
ls tests/queries/0_stateless/[0-9]*.reference | tail -n 1
```

Enquanto isso, alguns outros testes podem ser adicionados com o mesmo prefixo numérico, mas isso não tem problema e não causa nenhum inconveniente; você não precisa alterá-lo depois.

<div id="checking-for-an-error-that-must-occur">
  ### Verificando a ocorrência de um erro esperado
</div>

Às vezes, você pode querer testar se o servidor retorna um erro para uma consulta incorreta. Oferecemos suporte a anotações especiais para isso em testes SQL, no seguinte formato:

```sql
SELECT x; -- { serverError 49 }
```

Este teste garante que o servidor retorne um erro com código 49 informando que a coluna `x` é desconhecida.
Se não houver erro ou se o erro for diferente, o teste falhará.
Se você quiser garantir que um erro ocorra no lado do client, use a annotation `clientError`.

Não verifique uma redação específica da mensagem de erro, pois ela pode mudar no futuro e fazer o teste falhar desnecessariamente.
Verifique apenas o código de erro.
Se o código de erro existente não for específico o suficiente para as suas necessidades, considere adicionar um novo.

<div id="testing-a-distributed-query">
  ### Testando uma consulta distribuída
</div>

Se você quiser usar consultas distribuídas em testes funcionais, pode usar a função de tabela `remote` com endereços `127.0.0.{1..2}` para que o servidor faça consultas em si mesmo; ou usar clusters de teste predefinidos no arquivo de configuração do servidor, como `test_shard_localhost`.
Lembre-se de adicionar as palavras `shard` ou `distributed` ao nome do teste, para que ele seja executado no CI com as configurações corretas, nas quais o servidor está configurado para oferecer suporte a consultas distribuídas.

<div id="working-with-temporary-files">
  ### Trabalhando com arquivos temporários
</div>

Às vezes, em um teste de shell, pode ser necessário criar um arquivo temporariamente para usá-lo no teste.
Lembre-se de que algumas verificações de CI executam testes em paralelo, então, se você criar ou remover um arquivo temporário no seu script sem usar um nome único, isso pode fazer com que algumas verificações de CI, como Flaky, falhem.
Para contornar isso, use a variável de ambiente `$CLICKHOUSE_TEST_UNIQUE_NAME` para dar aos arquivos temporários um nome exclusivo para o teste em execução.
Assim, você pode ter certeza de que o arquivo criado durante a configuração ou removido durante a limpeza está sendo usado apenas por aquele teste, e não por outro teste executado em paralelo.

<div id="known-bugs">
  ## Bugs conhecidos
</div>

Se conhecemos bugs que podem ser facilmente reproduzidos por testes funcionais, colocamos testes funcionais preparados no diretório `tests/queries/bugs`.
Esses testes serão movidos para `tests/queries/0_stateless` quando os bugs forem corrigidos.

<div id="integration-tests">
  ## Testes de integração
</div>

Os testes de integração permitem testar o ClickHouse em uma configuração de cluster e a interação do ClickHouse com outros servidores, como MySQL, Postgres e MongoDB.
Eles são úteis para simular partições de rede, perda de pacotes etc.
Esses testes são executados no Docker e criam vários contêineres com diferentes softwares.

Consulte `tests/integration/README.md` para saber como executar esses testes.

Observe que a integração do ClickHouse com drivers de terceiros não é testada.
Além disso, no momento, não temos testes de integração com nossos drivers JDBC e ODBC.

<div id="unit-tests">
  ## Testes unitários
</div>

Os testes unitários são úteis quando você quer testar não o ClickHouse como um todo, mas uma única biblioteca ou classe de forma isolada.
Você pode habilitar ou desabilitar a compilação dos testes com a opção `ENABLE_TESTS` do CMake.
Os testes unitários (e outros programas de teste) ficam em subdiretórios `tests` espalhados pelo código.
Para executar os testes unitários, digite `ninja test`.
Alguns testes usam `gtest`, mas outros são apenas programas que retornam um código de saída diferente de zero em caso de falha no teste.

Não é necessário ter testes unitários se o código já estiver coberto por testes funcionais (e os testes funcionais geralmente são muito mais simples de usar).

Você pode executar verificações individuais do gtest chamando o executável diretamente, por exemplo:

```bash
$ ./src/unit_tests_dbms --gtest_filter=LocalAddress*
```

<div id="performance-tests">
  ## Testes de desempenho
</div>

Os testes de desempenho permitem medir e comparar o desempenho de alguma parte isolada do ClickHouse em consultas sintéticas.
Os testes de desempenho estão localizados em `tests/performance/`.
Cada teste é representado por um arquivo `.xml` com a descrição do caso de teste.
Os testes são executados com a ferramenta `docker/test/performance-comparison`. Consulte o arquivo readme para ver como executá-los.

Cada teste executa uma ou mais consultas (possivelmente com combinações de parâmetros) em loop.

Se você quiser melhorar o desempenho do ClickHouse em algum cenário, e se as melhorias puderem ser observadas em consultas simples, é altamente recomendável escrever um teste de desempenho.
Também é recomendável escrever testes de desempenho ao adicionar ou modificar funções SQL relativamente isoladas e não muito obscuras.
Sempre faz sentido usar `perf top` ou outras ferramentas `perf` durante os testes.

<div id="test-tools-and-scripts">
  ## Ferramentas e scripts de teste
</div>

Alguns programas no diretório `tests` não são testes prontos, mas ferramentas de teste.
Por exemplo, para `Lexer`, há uma ferramenta em `src/Parsers/tests/lexer` que apenas faz a tokenização de stdin e escreve o resultado colorido em stdout.
Você pode usar esse tipo de ferramenta como exemplo de código e para exploração e testes manuais.

<div id="miscellaneous-tests">
  ## Testes diversos
</div>

Há testes para modelos de aprendizado de máquina em `tests/external_models`.
Esses testes não recebem atualizações e precisam ser migrados para testes de integração.

Há um teste separado para inserções com quórum.
Esse teste executa um cluster do ClickHouse em servidores separados e emula vários casos de falha: partição de rede, perda de pacotes (entre nós do ClickHouse, entre o ClickHouse e o ZooKeeper, entre o servidor ClickHouse e o cliente etc.), `kill -9`, `kill -STOP` e `kill -CONT`, como no [Jepsen](https://aphyr.com/tags/Jepsen). Em seguida, o teste verifica que todas as inserções confirmadas foram gravadas e que nenhuma das inserções rejeitadas foi gravada.

<div id="manual-testing">
  ## Teste manual
</div>

Ao desenvolver um novo recurso, também faz sentido testá-lo manualmente.
Você pode fazer isso seguindo estas etapas:

Compile o ClickHouse. Execute o ClickHouse no terminal: mude para o diretório `programs/clickhouse-server` e execute-o com `./clickhouse-server`. Por padrão, ele usará a configuração (`config.xml`, `users.xml` e os arquivos nos diretórios `config.d` e `users.d`) do diretório atual. Para se conectar ao servidor ClickHouse, execute `programs/clickhouse-client/clickhouse-client`.

Observe que todas as ferramentas do ClickHouse (servidor, cliente etc.) são apenas links simbólicos para um único binário chamado `clickhouse`.
Você pode encontrar esse binário em `programs/clickhouse`.
Todas as ferramentas também podem ser invocadas como `clickhouse tool` em vez de `clickhouse-tool`.

Como alternativa, você pode instalar o pacote do ClickHouse: seja o lançamento estável do repositório do ClickHouse ou compilando você mesmo o pacote com `./release` na raiz do código-fonte do ClickHouse.
Em seguida, inicie o servidor com `sudo clickhouse start` (ou `stop` para parar o servidor).
Procure os logs em `/etc/clickhouse-server/clickhouse-server.log`.

Quando o ClickHouse já estiver instalado no seu sistema, você pode compilar um novo binário `clickhouse` e substituir o binário existente:

```bash
$ sudo clickhouse stop
$ sudo cp ./clickhouse /usr/bin/
$ sudo clickhouse start
```

Também é possível parar o clickhouse-server do sistema e executar sua própria instância com a mesma configuração, mas com logging no terminal:

```bash
$ sudo clickhouse stop
$ sudo -u clickhouse /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Exemplo com gdb:

```bash
$ sudo -u clickhouse gdb --args /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Se o `clickhouse-server` do sistema já estiver em execução e você não quiser pará-lo, poderá alterar os números de porta no seu `config.xml` (ou sobrescrevê-los em um arquivo no diretório `config.d`), definir o caminho de dados adequado e executá-lo.

O binário `clickhouse` quase não tem dependências e funciona em uma ampla variedade de distribuições Linux.
Para testar suas alterações de forma rápida e prática em um servidor, você pode simplesmente usar `scp` para copiar o binário `clickhouse` que acabou de compilar para o seu servidor e, em seguida, executá-lo como nos exemplos acima.

<div id="build-tests">
  ## Testes de compilação
</div>

Os testes de compilação permitem verificar se a compilação continua funcionando em várias configurações alternativas e em alguns outros sistemas.
Esses testes também são automatizados.

Exemplos:

* compilação cruzada para Darwin x86&#95;64 (macOS)
* compilação cruzada para FreeBSD x86&#95;64
* compilação cruzada para Linux AArch64
* compilação no Ubuntu com bibliotecas de pacotes do sistema (não recomendado)
* compilação com vinculação compartilhada de bibliotecas (não recomendado)

Por exemplo, a compilação com pacotes do sistema é uma prática ruim, porque não podemos garantir exatamente quais versões de pacotes um sistema terá.
Mas isso é realmente necessário para os mantenedores do Debian.
Por esse motivo, pelo menos precisamos oferecer suporte a essa variante de compilação.
Outro exemplo: a vinculação compartilhada é uma fonte comum de problemas, mas é necessária para alguns entusiastas.

Embora não possamos executar todos os testes em todas as variantes de compilação, queremos verificar pelo menos que as várias variantes de compilação continuem funcionando.
Para isso, usamos testes de compilação.

Também testamos se não há unidades de tradução longas demais para compilar ou que exijam RAM demais.

Também testamos se não há frames de pilha grandes demais.

<div id="testing-for-protocol-compatibility">
  ## Testando a compatibilidade do protocolo
</div>

Quando estendemos o protocolo de rede do ClickHouse, testamos manualmente se o clickhouse-client antigo funciona com o clickhouse-server novo e se o clickhouse-client novo funciona com o clickhouse-server antigo (simplesmente executando os binários dos pacotes correspondentes).

Também testamos alguns casos automaticamente com testes de integração:

* se os dados gravados pela versão antiga do ClickHouse podem ser lidos com sucesso pela nova versão;
* se as consultas distribuídas funcionam em um cluster com diferentes versões do ClickHouse.

<div id="help-from-the-compiler">
  ## Ajuda do compilador
</div>

O código principal do ClickHouse (localizado no diretório `src`) é compilado com `-Wall -Wextra -Werror` e com alguns avisos adicionais ativados.
No entanto, essas opções não são ativadas para bibliotecas de terceiros.

O Clang tem ainda mais avisos úteis — você pode procurá-los com `-Weverything` e selecionar alguns para a compilação padrão.

Sempre usamos clang para compilar o ClickHouse, tanto em desenvolvimento quanto em produção.
Você pode compilar na sua própria máquina no modo de depuração (para economizar a bateria do seu laptop), mas observe que o compilador consegue gerar mais avisos com `-O3` devido a uma análise de fluxo de controle e entre procedimentos melhor.
Ao compilar com clang no modo de depuração, é usada a versão de depuração de `libc++`, o que permite detectar mais erros em tempo de execução.

<div id="sanitizers">
  ## Sanitizers
</div>

:::note
Se o processo (servidor ou cliente do ClickHouse) encerrar inesperadamente ao iniciar quando executado localmente, talvez seja necessário desativar a randomização do layout do espaço de endereçamento: `sudo sysctl kernel.randomize_va_space=0`
:::

<div id="address-sanitizer">
  ### Address sanitizer
</div>

Executamos testes funcionais, de integração, de estresse e unitários com ASan a cada commit.

<div id="thread-sanitizer">
  ### Sanitizador de thread
</div>

Executamos testes funcionais, de integração, de estresse e unitários com TSan a cada commit.

<div id="memory-sanitizer">
  ### Sanitizador de memória
</div>

Executamos testes funcionais, de integração, de estresse e unitários sob o MSan a cada commit.

<div id="undefined-behaviour-sanitizer">
  ### Sanitizador de comportamento indefinido
</div>

Executamos testes funcionais, de integração, de estresse e unitários com UBSan a cada commit.
O código de algumas bibliotecas de terceiros não passa por sanitização para UB.

<div id="valgrind-memcheck">
  ### Valgrind (memcheck)
</div>

Costumávamos executar testes funcionais com o Valgrind durante a noite, mas isso não é mais feito.
Isso leva várias horas.
Atualmente, há um falso positivo conhecido na biblioteca `re2`; veja [este artigo](https://research.swtch.com/sparse).

<div id="fuzzing">
  ## Fuzzing
</div>

O fuzzing do ClickHouse é implementado tanto com [libFuzzer](https://llvm.org/docs/LibFuzzer.html) quanto com consultas SQL aleatórias.
Todos os testes de fuzzing devem ser executados com sanitizers (Address e Undefined).

O libFuzzer é usado para testes de fuzzing isolados do código de bibliotecas.
Os fuzzers são implementados como parte do código de teste e têm o sufixo &quot;&#95;fuzzer&quot; no nome.
Um exemplo de fuzzer pode ser encontrado em `src/Parsers/fuzzers/lexer_fuzzer.cpp`.
Configurações, dictionaries e corpus específicos do libFuzzer são armazenados em `tests/fuzz`.
Incentivamos você a escrever testes de fuzzing para toda funcionalidade que processe entradas do usuário.

Os fuzzers não são compilados por padrão.
Para compilar os fuzzers, as opções `-DENABLE_FUZZING=1` e `-DENABLE_TESTS=1` devem ser definidas.
Recomendamos desabilitar o Jemalloc ao compilar os fuzzers.
A configuração usada para integrar o fuzzing do ClickHouse ao
Google OSS-Fuzz pode ser encontrada em `docker/fuzz`.

Também usamos um teste de fuzzing simples para gerar consultas SQL aleatórias e verificar se o servidor não falha ao executá-las.
Você pode encontrá-lo em `00746_sql_fuzzy.pl`.
Esse teste deve ser executado continuamente (durante a noite e por mais tempo).

Também usamos um fuzzer sofisticado de consultas baseado em AST, capaz de encontrar uma enorme quantidade de casos extremos.
Ele faz permutações e substituições aleatórias na AST das consultas.
Ele armazena nós de AST de testes anteriores para usá-los no fuzzing de testes subsequentes, processando-os em ordem aleatória.
Você pode saber mais sobre esse fuzzer [neste artigo do blog](https://clickhouse.com/blog/fuzzing-click-house).

<div id="stress-test">
  ## Teste de estresse
</div>

Os testes de estresse são mais um caso de fuzzing.
Eles executam todos os testes funcionais em paralelo, em ordem aleatória, em um único servidor.
Os resultados dos testes não são verificados.

É verificado que:

* o servidor não trava, e nenhum trap de debug ou de sanitizer é disparado;
* não há deadlocks;
* a estrutura do banco de dados é consistente;
* o servidor pode ser encerrado com sucesso após o teste e iniciado novamente sem exceções.

Há cinco variantes (Debug, ASan, TSan, MSan, UBSan).

<div id="thread-fuzzer">
  ## Thread fuzzer
</div>

Thread Fuzzer (não confunda com o sanitizador de thread) é outro tipo de fuzzing que permite aleatorizar a ordem de execução das threads.
Ele ajuda a encontrar ainda mais casos de borda.

<div id="security-audit">
  ## Auditoria de segurança
</div>

Nossa equipe de Segurança fez uma análise inicial dos recursos do ClickHouse sob a ótica da segurança.

<div id="static-analyzers">
  ## Analisadores estáticos
</div>

Executamos o `clang-tidy` a cada commit.
As verificações do `clang-static-analyzer` também estão habilitadas.
O `clang-tidy` também é usado para algumas verificações de estilo.

Avaliamos `clang-tidy`, `Coverity`, `cppcheck`, `PVS-Studio`, `tscancode`, `CodeQL`.
Você encontrará instruções de uso no diretório `tests/instructions/`.

Se você usa o `CLion` como IDE, pode aproveitar algumas verificações do `clang-tidy` nativamente.

Também usamos o `shellcheck` para análise estática de scripts shell.

<div id="hardening">
  ## Hardening
</div>

Na compilação de depuração, usamos um alocador personalizado que aplica ASLR às alocações em nível de usuário.

Também protegemos manualmente regiões de memória que devem ser somente leitura após a alocação.

Na compilação de depuração, também usamos uma personalização da libc que garante que nenhuma função &quot;prejudicial&quot; (obsoleta, insegura, não thread-safe) seja chamada.

Asserções de depuração são amplamente usadas.

Na compilação de depuração, se for lançada uma exceção com o código &quot;logical error&quot; (o que indica um bug), o programa é encerrado imediatamente.
Isso permite usar exceções na compilação de lançamento, mas tratá-las como asserções na compilação de depuração.

A versão de depuração do jemalloc é usada para compilações de depuração.
A versão de depuração do libc++ é usada para compilações de depuração.

<div id="runtime-integrity-checks">
  ## Verificações de integridade em tempo de execução
</div>

Os dados armazenados em disco são verificados por checksum.
Os dados em tabelas MergeTree são verificados por checksum simultaneamente de três formas* (blocos de dados comprimidos, blocos de dados não comprimidos, o checksum total dos blocos).
Os dados transferidos pela rede entre cliente e servidor ou entre servidores também são verificados por checksum.
A replicação garante dados idênticos bit a bit nas réplicas.

Isso é necessário para proteger contra falhas de hardware (bit rot na mídia de armazenamento, inversões de bits na RAM do servidor, inversões de bits na RAM do controlador de rede, inversões de bits na RAM do switch de rede, inversões de bits na RAM do cliente, inversões de bits no wire).
Observe que inversões de bits são comuns e podem ocorrer mesmo com RAM ECC e na presença de checksum do TCP (se você chega a operar milhares de servidores processando petabytes de dados por dia).
[Veja o vídeo (russo)](https://www.youtube.com/watch?v=ooBAQIe0KlQ).

O ClickHouse fornece diagnósticos que ajudam engenheiros de operações a identificar hardware com falha.

* e isso não é lento.

<div id="code-style">
  ## Estilo de código
</div>

As regras de estilo de código são descritas [aqui](style.md).

Para verificar algumas violações comuns de estilo, você pode usar o script `utils/check-style`.

Para garantir o estilo adequado no seu código, você pode usar o `clang-format`.
O arquivo `.clang-format` fica na raiz do código-fonte.
Ele corresponde, em grande parte, ao nosso estilo de código atual.
Mas não é recomendável aplicar `clang-format` a arquivos existentes, porque isso piora a formatação.
Você pode usar a ferramenta `clang-format-diff`, que pode ser encontrada no repositório do código-fonte do clang.

Como alternativa, você pode tentar a ferramenta `uncrustify` para reformatar seu código.
A configuração está em `uncrustify.cfg`, na raiz do código-fonte.
Ela foi menos testada do que o `clang-format`.

O `CLion` tem seu próprio formatador de código, que precisa ser ajustado ao nosso estilo de código.

<div id="test-coverage">
  ## Cobertura de testes
</div>

Também monitoramos a cobertura de testes, mas apenas para os testes funcionais e somente para o clickhouse-server.
Isso é feito diariamente.

<div id="tests-for-tests">
  ## Testes para testes
</div>

Existe uma verificação automatizada para detectar testes instáveis.
Ela executa todos os novos testes 100 vezes (para testes funcionais) ou 10 vezes (para testes de integração).
Se o teste falhar pelo menos uma vez, ele é considerado instável.

<div id="test-automation">
  ## Automação de testes
</div>

Executamos testes com [GitHub Actions](https://github.com/features/actions).

Os jobs de compilação e os testes são executados no Sandbox a cada commit.
Os pacotes gerados e os resultados dos testes são publicados no GitHub e podem ser baixados por links diretos.
Os artefatos são armazenados por vários meses.
Quando você envia um pull request no GitHub, nós o marcamos como &quot;can be tested&quot;, e nosso sistema de CI compila pacotes do ClickHouse (release, debug, com address sanitizer etc.) para você.