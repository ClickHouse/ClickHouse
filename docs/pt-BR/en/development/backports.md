---
description: 'Visão geral da política de backport e da automação do ClickHouse'
sidebar_label: 'Backport System'
sidebar_position: 56
slug: /development/backports
title: 'Backport System'
doc_type: 'reference'
---

Este documento descreve a política de backport do ClickHouse e o sistema automatizado que a coloca em prática.

<div id="release-model">
  ## Modelo de lançamento
</div>

As versões do ClickHouse seguem o esquema `YY.M.patch.build-type`, em que `YY` é o ano com dois dígitos, `M` é o mês do lançamento (sem zero à esquerda), `patch` é o número do patch dentro da ramificação, `build` é um número de compilação que cresce monotonamente, e `type` é `stable` ou `lts`.

Exemplo: `25.3.8.23-lts` — LTS de março de 2025, patch 8, compilação 23.

Há duas linhas de lançamento:

* Os lançamentos **Stable** são publicados aproximadamente uma vez por mês. Os três lançamentos stable mais recentes recebem patches, o que resulta em cerca de três meses de suporte ativo por lançamento.
* Os lançamentos **LTS (Long-Term Support)** são publicados em março e agosto de cada ano. Duas versões LTS têm suporte simultaneamente, cada uma por pelo menos 12 meses.

Recomenda-se que os usuários que executam workloads de production usem a versão stable mais recente ou um lançamento LTS e atualizem prontamente para novas versões de patch, já que lançamentos de patch não introduzem breaking changes.

<div id="backport-policy">
  ## Política de backport
</div>

Nem todas as mudanças recebem backport. O objetivo é manter as branches de lançamento estáveis, portanto o escopo dos backports é intencionalmente restrito:

* **Correções de segurança** — sempre recebem backport.
* **Correções de bugs críticos** (exceptions (logical errors), perda de dados, resultados incorretos, problemas de RBAC) — são selecionadas automaticamente para backport de acordo com as regras gerais; identificadas pelo rótulo `pr-critical-bugfix`, que faz com que `pr-must-backport` seja adicionado automaticamente.
* **Correções de estabilidade e regressão** — recebem backport quando o risco da mudança é baixo em comparação com o risco de manter o bug; identificadas por `pr-must-backport`, adicionado manualmente pelos mantenedores.
* **Correções de bugs menores com solução alternativa disponível** — em geral não recebem backport, para evitar desestabilizar as branches de lançamento.
* **Novos recursos, melhorias e trabalho de desempenho** — não recebem backport.

O rótulo `pr-must-backport` é o override manual usado pelos mantenedores para marcar um PR para backport. O rótulo `pr-critical-bugfix` faz com que `pr-must-backport` seja adicionado automaticamente pelo hook de CI (consulte `pr_labels_and_category.py`).

**Escalonamento de conflitos.** Quando o backport automático não consegue resolver conflitos de merge, ainda assim um PR de cherry-pick deve ser criado e atribuído ao autor, a quem fez o merge e aos responsáveis já atribuídos no PR original, para que alguém possa resolver os conflitos e concluir o backport.

<div id="backport-tool">
  ## Ferramenta de Backport
</div>

A política de backport descrita acima é implementada pela ferramenta automatizada em `tests/ci/cherry_pick.py`. A ferramenta é executada como um workflow do GitHub Actions na infraestrutura do ClickHouse e cobre todos os requisitos: descobrir branches de lançamento ativas, selecionar PRs elegíveis para backport, executar o procedimento de cherry-pick e backport em duas etapas, gerenciar conflitos, aplicar a política de atraso e manter os rótulos sincronizados.

O objetivo de longo prazo é extrair essa implementação para uma ferramenta Python independente de código aberto que outros projetos possam adotar. O design pretendido é:

* **Configurável** — todos os parâmetros da política (rótulos de qualificação, janela de atraso, limiares para PRs stale, comportamento durante rolling-out etc.) expressos em um arquivo de configuração, para que a ferramenta possa ser adaptada aos requisitos de backport de qualquer projeto sem alterações no código.
* **Distribuível** — empacotada como um wheel Python autocontido instalável via PyPI, sem dependência da infraestrutura de CI do ClickHouse.
* **Programável** — expondo um modelo de objetos claro para pull requests, rótulos e branches de lançamento, para que os usuários possam criar scripts e workflows personalizados sobre o mecanismo principal.

<div id="testing">
  ### Testes
</div>

Uma parte planejada da ferramenta autônoma é uma suíte de testes dedicada, juntamente com uma infraestrutura de testes enxuta. Essa infraestrutura poderá criar repositórios temporários no GitHub (ou equivalentes locais) pré-populados com:

* um conjunto configurável de branches que representam linhas de release,
* pull requests com diferentes combinações de labels de backport,
* PRs de release com o label `release` apontando para as branches de release.

Isso permite que os testes exercitem todo o fluxo de automação — detecção de labels, criação de branch de cherry-pick, tratamento de conflitos, criação de PR de backport, lógica de atribuição, ignorar rolling-out e política de atraso — em um repositório real, mas descartável, sem afetar o estado de produção. A mesma infraestrutura também pode ser reutilizada para fazer testes de regressão em alterações de política antes de serem implantadas.

<div id="active-release-branches">
  ## Branches de lançamento ativas
</div>

Uma branch de lançamento ativa é qualquer branch cuja PR de lançamento correspondente (com o rótulo `release`) ainda esteja aberta no GitHub. A automação de backport identifica isso dinamicamente a cada execução, portanto não é necessária nenhuma alteração de configuração quando um novo lançamento é criado ou um antigo chega ao fim de vida.

Uma branch de lançamento pode estar no estado de **rolling-out** (sua PR de lançamento contém o rótulo `rolling-out`) durante o período em que um novo lançamento está sendo implantado. Os backports gerais ficam pausados para branches em rolling-out, para evitar complicar o rollout. Rótulos específicos de versão (por exemplo, `v25.3-must-backport`) substituem isso e forçam o backport mesmo durante um rollout.

Um rótulo específico de versão define o lançamento *mais antigo* que a PR precisa alcançar: o backport é feito para esse lançamento **e para todas as branches de lançamento ativas mais recentes**, não apenas para a branch nomeada. Por exemplo, `v25.3-must-backport` em uma PR mesclada na branch de desenvolvimento faz backport para `25.3` e para todos os lançamentos ativos posteriores (`25.4`, `25.5`, …). Se houver vários rótulos específicos de versão, a menor versão prevalece, já que ela já abrange as mais novas.

O lançamento nomeado não precisa estar ativo. Um rótulo para um lançamento em fim de vida (sem nenhuma PR de lançamento aberta) ainda leva a correção adiante para todos os lançamentos ativos posteriores, para que uma atualização a partir desse lançamento nunca deixe de incluir a correção sem aviso. Por exemplo, `v25.12-must-backport` em uma PR continua fazendo backport para `26.1`, `26.2`, … mesmo depois de o próprio `25.12` ter chegado ao fim de vida.

<div id="implementation">
  ## Implementação
</div>

<div id="overview">
  ### Visão geral
</div>

A automação de backport é executada a cada hora como o workflow `CherryPick` do GitHub Actions (`.github/workflows/cherry_pick.yml`), implementado em `tests/ci/cherry_pick.py`. Ela opera por meio da API do GitHub e de operações locais do git em um runner `style-checker-aarch64` auto-hospedado.

O processo tem duas etapas para cada par (PR original, branch de lançamento):

1. Um **PR de cherry-pick** é criado para isolar a resolução de conflitos do destino real do merge. Se não houver conflitos, ele será mesclado automaticamente.
2. Um **PR de backport** é criado tendo como destino a branch de lançamento real, com as alterações do cherry-pick consolidadas em um único commit.

<div id="labels">
  ### Rótulos
</div>

Os rótulos no PR original controlam se o backport acontece e para quais branches ele vai.

| Rótulo                                                      | Efeito                                                                                                                                                                                                                                                                                                                              |
| ----------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pr-must-backport`                                          | Backport para todos os branches de lançamento ativos (exceto os branches marcados como `rolling-out`)                                                                                                                                                                                                                                     |
| `pr-must-backport-force`                                    | Backport para todos os branches de lançamento ativos, ignorando as restrições de `rolling-out`                                                                                                                                                                                                                                            |
| `pr-critical-bugfix`                                        | Aciona `pr-must-backport` automaticamente (via `AUTO_BACKPORT` em `pr_labels_and_category.py`)                                                                                                                                                                                                                                      |
| `v{VER}-must-backport` (por exemplo, `v25.3-must-backport`) | Backport para esse branch de lançamento **e todos os branches de lançamento ativos mais recentes** — a versão marca o lançamento *mais antigo* ao qual o PR deve chegar, mesmo quando o lançamento nomeado já está em fim de vida. Com vários rótulos desse tipo, prevalece a menor versão. Ignora a exclusão por `rolling-out` nesses branches |
| `pr-backports-created`                                      | Definido pelo bot quando todos os PRs de backport necessários tiverem sido criados; removido se um PR de cherry-pick for reaberto                                                                                                                                                                                                   |
| `pr-cherrypick`                                             | Aplicado a PRs de cherry-pick criados pelo bot                                                                                                                                                                                                                                                                                      |
| `pr-backport`                                               | Aplicado a PRs de backport criados pelo bot                                                                                                                                                                                                                                                                                         |
| `do not test`                                               | Aplicado a PRs de cherry-pick para que a CI não seja executada neles                                                                                                                                                                                                                                                                |
| `rolling-out`                                               | Definido em um **release PR** para indicar que seu branch está sendo implantado no momento; backports gerais o ignoram                                                                                                                                                                                                              |

<div id="branch-and-pr-naming">
  ### Nomenclatura de branch e PR
</div>

Para cada PR original de número `N` e branch de lançamento `release/X.Y`:

* Branch de cherry-pick: `cherrypick/release/X.Y/N`
* Branch de backport: `backport/release/X.Y/N`
* Título do PR de cherry-pick: `Cherry pick #N to release/X.Y: <original title>`
* Título do PR de backport: `Backport #N to release/X.Y: <original title>`

<div id="step-by-step-process">
  ### Processo passo a passo
</div>

<div id="discover-active-releases">
  #### 1. Descobrir lançamentos ativos
</div>

`BackportPRs.receive_release_prs` consulta o GitHub em busca de todos os PRs abertos com o rótulo `release`. As refs `head` desses PRs são os nomes dos branches de lançamento (por exemplo, `release/25.3`). A partir delas, ele determina o conjunto de rótulos específicos de versão a serem procurados: todo rótulo `v{VER}-must-backport` que exista no repositório e cuja versão não seja mais recente do que o lançamento ativo mais recente. Rótulos mais antigos são incluídos mesmo quando seu lançamento não está mais ativo (um rótulo mais recente do que todos os lançamentos ativos é ignorado, já que não poderia se expandir para nenhum branch ativo), de modo que um PR rotulado para um lançamento em fim de vida ainda seja encontrado, desde que exista um lançamento ativo mais recente.

<div id="find-prs-to-backport">
  #### 2. Encontrar PRs para backport
</div>

`BackportPRs.receive_prs_for_backport` usa a API de busca do GitHub para localizar PRs mesclados que:

* tenham pelo menos um rótulo de backport (`pr-must-backport`, `pr-must-backport-force`, `pr-critical-bugfix` ou um rótulo específico de versão), e
* **não** tenham ainda `pr-backports-created`, e
* tenham sido mesclados após a data do commit mais antigo encontrada em qualquer branch de lançamento, e
* tenham sido atualizados nos últimos 90 dias (para manter a consulta de busca eficiente).

<div id="rolling-out-branch-handling">
  #### 3. Tratamento de branch com `rolling-out`
</div>

Quando um PR de lançamento recebe o rótulo `rolling-out`, os rótulos gerais de backport (`pr-must-backport`, `pr-critical-bugfix`) ignoram essa branch. O bot fecha todos os PRs de cherry-pick ou de backport criados anteriormente para essa branch com um comentário explicativo. Um rótulo específico de versão (por exemplo, `v25.3-must-backport`) sempre prevalece sobre isso — para o lançamento indicado e para cada branch de lançamento ativa mais recente à qual ele se aplica. `pr-must-backport-force` ignora a verificação de `rolling-out` em todas as branches.

<div id="cherry-pick-stage">
  #### 4. Etapa de cherry-pick (`ReleaseBranch.create_cherrypick`)
</div>

Para cada par (PR original, branch de lançamento) em que ainda não exista uma PR de cherry-pick:

1. Faça checkout da branch de lançamento e crie uma **branch de backport** (`backport/release/X.Y/N`) a partir dela.
2. Execute `git merge -s ours` em relação ao primeiro parent do commit de merge para criar uma base de merge sintética, sem alterações no conteúdo.
3. Crie à força uma **branch de cherry-pick** (`cherrypick/release/X.Y/N`) apontando diretamente para o commit de merge da PR original.
4. Tente fazer `git merge --no-commit --no-ff` da branch de cherry-pick na branch de backport:
   * Se já estiver tudo atualizado, a alteração já está presente na branch de lançamento — marque como concluído e pule esta etapa.
   * Caso contrário (com ou sem conflitos), faça reset e envie ambas as branches.
5. Crie a PR de cherry-pick com destino a `backport/release/X.Y/N`, a partir de `cherrypick/release/X.Y/N`, com os labels `pr-cherrypick` e `do not test`.
6. Propague `pr-bugfix` ou `pr-critical-bugfix` da PR original, se aplicável.
7. Os responsáveis **não** são definidos neste ponto; eles só são adicionados quando conflitos são detectados.

<div id="auto-merge-conflict-free-cherry-pick-prs">
  #### 5. Mesclagem automática de PRs de cherry-pick sem conflitos
</div>

Se o PR de cherry-pick puder ser mesclado (sem conflitos), o bot faz o merge automaticamente pela API do GitHub e prossegue imediatamente para a etapa de backport.

<div id="backport-stage">
  #### 6. Etapa de backport (`ReleaseBranch.create_backport`)
</div>

Depois que o PR de cherry-pick for mesclado:

1. Faça checkout da backport branch e atualize-a com `git pull`.
2. Encontre a merge-base entre a branch de lançamento e a backport branch.
3. Execute `git reset --soft` até a merge-base, consolidando em um único commit todos os commits aplicados com cherry-pick.
4. Faça commit usando o título do PR de backport como mensagem.
5. Faça force-push da backport branch e abra um PR de backport tendo como destino a branch de lançamento real.
6. Adicione ao PR o label `pr-backport` (e `pr-bugfix` / `pr-critical-bugfix`, se aplicável).
7. Atribua o PR ao autor do PR original, a quem fez o merge e aos responsáveis já atribuídos (excluindo contas de robô).

<div id="completion">
  #### 7. Conclusão
</div>

Quando o backport é aplicado a todas as branches de lançamento de um determinado PR original, o bot adiciona `pr-backports-created` ao PR original.

<div id="pre-check">
  #### 8. Pré-verificação
</div>

Antes de iniciar qualquer trabalho em uma PR, `ReleaseBranch.pre_check` executa `git merge-base --is-ancestor` para verificar se o commit de merge ainda não está presente na branch de lançamento. Se estiver, a PR é considerada já submetida a backport e é ignorada.

<div id="stale-cherry-pick-pr-handling">
  ### Tratamento de PRs de cherry-pick sem atividade
</div>

A classe `CherryPickPRs` é executada no início de cada execução horária e trata de dois cenários:

* **PRs de cherry-pick órfãos**: se a release branch de um PR de cherry-pick não tiver mais um PR de lançamento aberto (ou seja, se o lançamento estiver encerrado), o PR de cherry-pick será fechado automaticamente.
* **PRs de cherry-pick reabertos**: se um PR original já tiver `pr-backports-created`, mas um PR de cherry-pick correspondente ainda estiver aberto, o rótulo `pr-backports-created` será removido do PR original para que ele possa ser reprocessado.

Para PRs de cherry-pick que aguardam resolução manual de conflitos:

* Após **3 dias** sem atualizações, o bot publica um comentário de ping mencionando os responsáveis atribuídos.
* Após **7 dias** sem atualizações, o bot publica um comentário de encerramento e fecha o PR.

<div id="conflict-resolution">
  ### Resolução de conflitos
</div>

Quando um cherry-pick gera conflitos, a PR de cherry-pick permanece aberta para que alguém os resolva. O bot a atribui ao autor da PR original, a quem fez o merge e aos responsáveis atribuídos. Depois que os conflitos são resolvidos e a PR de cherry-pick sofre merge, o bot cria a PR de backport na próxima execução horária.

Para descartar um backport por completo, feche a PR de cherry-pick. O bot a tratará como intencionalmente ignorada.

Para recriar do zero uma PR de cherry-pick com problema:

1. Remova a label `pr-cherrypick` da PR de cherry-pick.
2. Exclua a branch `cherrypick/...`.
3. Remova `pr-backports-created` da PR original, se estiver presente.

<div id="ci-for-backport-prs">
  ### CI para PRs de backport
</div>

PRs de backport são direcionados a branches de lançamento, por isso usam um workflow de CI dedicado (`BackportPR`, definido em `ci/workflows/backport_branches.py`) em vez do workflow padrão de pull request. Esse workflow executa um subconjunto representativo da CI: compilações ASan/UBSan e TSan, compilações de lançamento, compilações para macOS, testes funcionais com ASan, testes de estresse com TSan e testes de integração. Ele valida que a branch de backport tem entre 1 e 50 commits e pelo menos um arquivo alterado (verificado por `check_backport_branch.py`).

<div id="authentication">
  ### Autenticação
</div>

O workflow usa uma chave SSH (`ROBOT_CLICKHOUSE_SSH_KEY`) para operações de git push. As chamadas da API do GitHub são autenticadas via `get_best_robot_token`, que seleciona o token com a maior cota restante de um conjunto armazenado no SSM (`/github-tokens`). `ROBOT_CLICKHOUSE_COMMIT_TOKEN` é usado na etapa de checkout do workflow do Actions, não para chamadas de API. As contas de robô (`robot-clickhouse`, `clickhouse-gh`) são excluídas ao atribuir uma pessoa responsável.

<div id="github-api-cache">
  ### Cache da API do GitHub
</div>

`GitHubCache` (de `cache_utils.py`) persiste o cache de objetos do PyGithub no S3, reduzindo as chamadas à API entre execuções feitas de hora em hora. O cache é baixado no início e carregado ao final de cada execução.

<div id="error-handling">
  ### Tratamento de erros
</div>

Os erros durante o processamento individual dos PRs são capturados e registrados, mas não interrompem a execução. Depois que todos os PRs forem processados, se tiver ocorrido algum erro, será gerada uma `BackportException`. Em CI, isso aciona uma notificação via `CIBuddy` no chat da equipe.