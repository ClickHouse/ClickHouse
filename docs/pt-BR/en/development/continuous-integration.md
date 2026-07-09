---
description: 'Visão geral do sistema de integração contínua do ClickHouse'
sidebar_label: 'Integração Contínua (CI)'
sidebar_position: 55
slug: /development/continuous-integration
title: 'Integração Contínua (CI)'
doc_type: 'reference'
---

Ao enviar um pull request, algumas verificações automatizadas são executadas no seu código pelo [sistema de integração contínua (CI)](tests.md#test-automation) do ClickHouse.
Isso acontece depois que um mantenedor do repositório (alguém da equipe da ClickHouse) faz uma triagem do seu código e adiciona o rótulo `can be tested` ao seu pull request.
Os resultados das verificações são exibidos na página do pull request no GitHub, conforme descrito na [documentação do GitHub sobre verificações de status](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-status-checks).
Se uma verificação falhar, talvez seja necessário corrigi-la.
Esta página apresenta uma visão geral das verificações que você pode encontrar e do que fazer para corrigi-las.

Se parecer que a falha na verificação não está relacionada às suas alterações, pode ser uma falha transitória ou um problema de infraestrutura.
Envie um commit vazio para o pull request para reiniciar as verificações de CI:

```shell
git commit --allow-empty
git push
```

Se não tiver certeza do que fazer, peça ajuda a um mantenedor.

<div id="merge-with-master">
  ## Merge com master
</div>

Verifica se a PR pode ser mesclada na master.
Caso contrário, falhará com a mensagem `Cannot fetch mergecommit`.
Para corrigir essa verificação, resolva o conflito conforme descrito na [documentação do GitHub](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/resolving-a-merge-conflict-on-github) ou faça merge da branch `master` na branch da sua pull request usando git.

<div id="docs-check">
  ## Verificação da documentação
</div>

Tenta compilar o site de documentação do ClickHouse.
Pode falhar se você tiver alterado algo na documentação.
O motivo mais provável é que alguma referência cruzada na documentação esteja incorreta.
Acesse o relatório da verificação e procure as mensagens `ERROR` e `WARNING`.

<div id="description-check">
  ## Verificação da descrição
</div>

Verifique se a descrição do seu pull request está em conformidade com o template [PULL&#95;REQUEST&#95;TEMPLATE.md](https://github.com/ClickHouse/ClickHouse/blob/master/.github/PULL_REQUEST_TEMPLATE.md).
Você precisa especificar uma categoria de changelog para a sua alteração (por exemplo, correção de bug) e escrever uma mensagem clara para o usuário descrevendo a alteração em [CHANGELOG.md](../whats-new/changelog/index.md)

<div id="docker-image">
  ## Imagem Docker
</div>

Compila as imagens Docker do servidor ClickHouse e do Keeper para verificar se a compilação ocorre corretamente.

<div id="official-docker-library-tests">
  ### Testes oficiais da biblioteca Docker
</div>

Executa os testes da [biblioteca oficial do Docker](https://github.com/docker-library/official-images/tree/master/test#alternate-config-files) para verificar se a imagem Docker `clickhouse/clickhouse-server` funciona corretamente.

Para adicionar novos testes, crie um diretório `ci/jobs/scripts/docker_server/tests/$test_name` e o script `run.sh` nesse diretório.

Mais detalhes sobre os testes podem ser encontrados na [documentação dos scripts dos jobs de CI](https://github.com/ClickHouse/ClickHouse/tree/master/ci/jobs/scripts/docker_server).

<div id="marker-check">
  ## Verificação Marker
</div>

Esta verificação significa que o sistema de CI começou a processar o pull request.
Quando ela está com o status &#39;pending&#39;, isso significa que nem todas as verificações foram iniciadas.
Depois que todas as verificações forem iniciadas, o status muda para &#39;success&#39;.

<div id="style-check">
  ## Verificação de estilo
</div>

Executa várias verificações de estilo no código-fonte. Cada subverificação abaixo corresponde a um `testname` em [`ci/jobs/check_style.py`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/check_style.py) e pode ser executada individualmente com `--test <name>` (veja abaixo).

<div id="cpp">
  ##### cpp
</div>

Verificações de estilo de C++ baseadas em Regex por meio do [`check_cpp.sh`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/scripts/check_style/check_cpp.sh). Se falhar, corrija os problemas de acordo com o [guia de estilo de código](style.md).

<div id="whitespace-check">
  ##### whitespace_check
</div>

Sinaliza espaços duplos após vírgulas em C++ que não fazem parte do alinhamento de colunas.

<div id="catch-all">
  ##### catch_all
</div>

Proíbe `catch (...)` fora de destrutores, de `main` e de pontos de entrada do fuzzer, nos quais ignorar uma exceção desconhecida não é seguro.

<div id="yamllint">
  ##### yamllint
</div>

Faz lint dos arquivos YAML de workflow em `.github/` usando `.yamllint`.

<div id="xmllint">
  ##### xmllint
</div>

Valida arquivos XML em `tests/` e `programs/`.

<div id="functional-tests-check">
  ##### functional_tests_check
</div>

Verifica testes sem estado: consultas que filtram por `event_date` devem usar `>= yesterday()` em vez de `today()` (para evitar instabilidade em torno da meia-noite), e os nomes dos arquivos de teste não devem conter `fail`.

<div id="test-numbers-check">
  ##### test_numbers_check
</div>

Sinaliza grandes lacunas na numeração de testes sem estado (`tests/queries/0_stateless/<NNNNN>_*`).

<div id="symlinks">
  ##### links simbólicos
</div>

Detecta links simbólicos quebrados no repositório.

<div id="various">
  ##### diversos
</div>

Verificações diversas no repositório via [`various_checks.sh`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/scripts/check_style/various_checks.sh): consultas em `system.query_log` / `system.parts` / etc. devem filtrar por `currentDatabase`; caminhos do ZooKeeper de `Replicated*MergeTree` devem incluir um prefixo específico por teste; diretórios de testes de integração devem ter `__init__.py`; não pode haver BOM UTF, nem bits executáveis em arquivos de código-fonte/dados, nem tags `:latest` em imagens de terceiros no docker-compose; entre outras verificações.

<div id="running-style-check-locally">
  ### Executando localmente o job Verificação de estilo
</div>

Todo o job *Verificação de estilo* pode ser executado localmente em um contêiner Docker com:

```sh
python -m ci.praktika run "Style check"
```

Para executar uma verificação específica (por exemplo, a verificação *cpp*):

```sh
python -m ci.praktika run "Style check" --test cpp
```

Esses comandos baixam a imagem Docker `clickhouse/style-test` e executam o job em um contêiner.
Não é necessária nenhuma dependência além de Python 3 e Docker.

<div id="running-stateless-tests">
  ## Executando testes sem estado
</div>

Uma instalação local do ClickHouse com as configurações padrão pode funcionar em casos de teste específicos, mas não consegue executar corretamente todas as consultas de teste. Em CI, cada job instala uma configuração específica do ClickHouse (por exemplo, armazenamento S3, Parallel Replicas), o que pode ser trabalhoso de reproduzir manualmente. Para evitar isso, você pode reproduzir localmente qualquer job da CI usando a mesma orquestração da CI — sem necessidade de configuração manual.

<div id="ci-prerequisites">
  #### Pré-requisitos
</div>

* Python 3 (apenas biblioteca padrão)
* Docker

Instale o Docker no Ubuntu, se necessário, e faça login novamente:

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

<div id="run-ci-job-locally">
  #### Executar um job de CI localmente
</div>

Escolha o nome de qualquer job em um relatório de CI e execute-o localmente:

```bash
python -m ci.praktika run "<JOB_NAME>"
```

* Sempre coloque entre aspas o nome do job exatamente como ele aparece no relatório da CI (ele pode conter espaços e vírgulas), por exemplo: `"Stateless tests (amd_debug, parallel)"`. Isso configura o ClickHouse da mesma forma e executa os mesmos testes da CI.
* A arquitetura e o tipo de compilação no nome do job (por exemplo, `amd_debug`) são rótulos específicos da CI. Ao executar localmente, eles não têm efeito — o job usará qualquer binário que você fornecer, em qualquer arquitetura em que estiver executando. O nome do job determina apenas a configuração do ClickHouse e o conjunto de testes (a menos que isso seja substituído por `--test`).
* Na CI, os testes funcionais são divididos em lotes para melhor utilização de recursos. Por exemplo, `"Stateless tests (amd_debug, parallel)"` e `"Stateless tests (amd_debug, sequential)"` juntos cobrem todo o escopo: os testes seguros para paralelização são executados de forma concorrente, e o restante é executado de forma sequencial. Essa divisão reduz o tempo total da CI ao maximizar o paralelismo sempre que possível. Para reproduzir localmente todo o escopo dos testes, execute ambos os lotes.
* Também existe um job de CI `"Fast test"` que executa um escopo limitado de testes funcionais para verificar a funcionalidade básica do ClickHouse — ele usa uma compilação sem todos os módulos opcionais e é a forma mais rápida de detectar regressões. Você pode executá-lo localmente da mesma maneira. Coloque o binário do ClickHouse em um dos caminhos de busca padrão (`./ci/tmp/clickhouse`, `./build/programs/clickhouse` ou `./clickhouse`) — caso contrário, o job tentará compilar o ClickHouse primeiro:
  ```bash
  python -m ci.praktika run "Fast test"
  ```

<div id="run-specific-tests-within-ci-job">
  #### Executar testes específicos em um job de CI
</div>

Com `--test`, o job prepara uma configuração do ClickHouse idêntica à usada na CI, mas executa apenas os testes selecionados:

```bash
python -m ci.praktika run "Stateless tests (amd_debug, parallel)" \
  --test 00001_select1
```

* Você pode informar vários nomes de teste:
  ```bash
  python -m ci.praktika run "Stateless tests (amd_debug, parallel)" \
    --test 00001_select1 00002_log_and_exception_messages_formatting
  ```
* Dica: se qualquer configuração do ClickHouse servir e você só precisar executar testes específicos, use o alias `functional` em vez do nome completo do job:
  ```bash
  python -m ci.praktika run functional --test 00001_select1
  ```

<div id="additional-customization-options">
  #### Opções adicionais de personalização
</div>

* `--path PATH` — caminho personalizado para o binário do ClickHouse. Por padrão, o runner procura nesta ordem: `./ci/tmp/clickhouse`, `./build/programs/clickhouse`, `./clickhouse`.
* `--count N` — repete cada teste N vezes.
* `--workers N` — substitui o cálculo automático do número de workers paralelos com base na capacidade da máquina.

<div id="build-check">
  ## Verificação de compilação
</div>

Compila o ClickHouse em várias configurações para uso nas próximas etapas.

<div id="running-builds-locally">
  ### Executando compilações localmente
</div>

A compilação pode ser executada localmente em um ambiente semelhante ao de CI com:

```bash
python -m ci.praktika run "<BUILD_JOB_NAME>"
```

Não são necessárias dependências além de Python 3 e Docker.

<div id="available-build-jobs">
  #### Jobs de compilação disponíveis
</div>

Os nomes dos jobs de compilação são exatamente os mesmos que aparecem no Relatório de CI:

**Compilações AMD64:**

* `Build (amd_debug)` - Compilação de depuração com símbolos
* `Build (amd_release)` - Compilação de lançamento otimizada
* `Build (amd_asan)` - Compilação com Address Sanitizer
* `Build (amd_tsan)` - Compilação com sanitizador de thread
* `Build (amd_msan)` - Compilação com Memory Sanitizer
* `Build (amd_ubsan)` - Compilação com Undefined Behavior Sanitizer
* `Build (amd_binary)` - Compilação de lançamento rápida sem Thin LTO
* `Build (amd_compat)` - Compilação de compatibilidade para sistemas mais antigos
* `Build (amd_musl)` - Compilação com musl libc
* `Build (amd_darwin)` - Compilação para macOS
* `Build (amd_freebsd)` - Compilação para FreeBSD

**Compilações ARM64:**

* `Build (arm_release)` - Compilação de lançamento otimizada para ARM64
* `Build (arm_asan)` - Compilação com Address Sanitizer para ARM64
* `Build (arm_coverage)` - Compilação ARM64 com instrumentação de cobertura
* `Build (arm_binary)` - Compilação de lançamento rápida para ARM64 sem Thin LTO
* `Build (arm_darwin)` - Compilação ARM64 para macOS
* `Build (arm_v80compat)` - Compilação de compatibilidade com ARMv8.0

**Outras arquiteturas:**

* `Build (ppc64le)` - PowerPC de 64 bits Little Endian
* `Build (riscv64)` - RISC-V de 64 bits
* `Build (s390x)` - IBM System/390 de 64 bits
* `Build (loongarch64)` - LoongArch de 64 bits

Se o job for concluído com sucesso, os resultados da compilação estarão disponíveis no diretório `<repo_root>/ci/tmp/build`.

**Observação:** Para compilações fora da categoria &quot;Outras arquiteturas&quot; (que usam compilação cruzada), a arquitetura da sua máquina local deve corresponder ao tipo de compilação para que ela seja gerada conforme solicitado por `BUILD_JOB_NAME`.

<div id="example-run-local">
  #### Exemplo
</div>

Para executar uma compilação de depuração local:

```bash
python -m ci.praktika run "Build (amd_debug)"
```

Se a abordagem acima não funcionar no seu caso, use as opções do `cmake` do log de compilação e siga o [processo geral de compilação](../development/build.md).

<div id="functional-stateless-tests">
  ## Testes funcionais sem estado
</div>

Executa [testes funcionais sem estado](tests.md#functional-tests) para binários do ClickHouse compilados em várias configurações -- release, debug, com sanitizers etc.
Consulte o relatório para ver quais testes falharam e, em seguida, reproduza a falha localmente, conforme descrito [aqui](/pt-BR/development/tests#functional-tests).
Observe que é necessário usar a configuração de compilação correta para reproduzir o problema -- um teste pode falhar com AddressSanitizer, mas passar em Debug.
Baixe o binário na [página de verificações de compilação do CI](/pt-BR/install/advanced) ou compile-o localmente.

<div id="integration-tests">
  ## Testes de integração
</div>

Executa [testes de integração](tests.md#integration-tests).

<div id="bugfix-validate-check">
  ## Verificação de validação de bugfix
</div>

Verifica se há um novo teste (funcional ou de integração) ou testes alterados que falham com o binário compilado a partir da branch master.
Esta verificação é acionada quando o pull request tem o rótulo &quot;pr-bugfix&quot;.

<div id="stress-test">
  ## Teste de estresse
</div>

Executa testes funcionais sem estado de forma concorrente a partir de vários clientes para detectar erros relacionados à concorrência. Se falhar:

* Corrija primeiro todas as outras falhas de teste;
  * Consulte o relatório para localizar os logs do servidor e verificá-los em busca de possíveis causas
    do erro.

<div id="compatibility-check">
  ## Verificação de compatibilidade
</div>

Verifica se o binário `clickhouse` funciona em distribuições com versões antigas da libc.
Se falhar, peça ajuda a um mantenedor.

<div id="ast-fuzzer">
  ## AST fuzzer
</div>

Executa consultas geradas aleatoriamente para identificar erros no programa.
Se falhar, peça ajuda a um mantenedor.

<div id="performance-tests">
  ## Testes de desempenho
</div>

Meça as variações no desempenho das consultas.
Esta é a verificação mais demorada e leva pouco menos de 6 horas para ser executada.
O relatório do teste de desempenho está descrito em detalhes [aqui](https://github.com/ClickHouse/ClickHouse/blob/master/tests/performance/scripts/README.md#how-to-read-the-report).