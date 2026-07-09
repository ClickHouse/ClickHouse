---
description: 'Documentação sobre upgrade'
sidebar_title: 'Upgrade autogerenciado'
slug: /operations/update
title: 'Upgrade autogerenciado'
doc_type: 'guide'
---

<div id="clickhouse-upgrade-overview">
  ## Visão geral da atualização do ClickHouse
</div>

Este documento contém:

* diretrizes gerais
* um plano recomendado
* detalhes específicos sobre como atualizar os binários em seus sistemas

<div id="general-guidelines">
  ## Diretrizes gerais
</div>

Estas observações devem ajudar você no planejamento e a entender por que fazemos as recomendações apresentadas mais adiante no documento.

<div id="upgrade-clickhouse-server-separately-from-clickhouse-keeper-or-zookeeper">
  ### Faça o upgrade do ClickHouse server separadamente do ClickHouse Keeper ou ZooKeeper
</div>

A menos que seja necessária uma correção de segurança no ClickHouse Keeper ou no Apache ZooKeeper, não é necessário fazer upgrade do Keeper ao fazer upgrade do ClickHouse server. A estabilidade do Keeper é essencial durante o processo de upgrade; portanto, conclua os upgrades do ClickHouse server antes de considerar um upgrade do Keeper.

<div id="minor-version-upgrades-should-be-adopted-often">
  ### Upgrades de versões secundárias devem ser adotados com frequência
</div>

É altamente recomendável sempre fazer upgrade para a versão secundária mais recente assim que ela for lançada. Lançamentos secundários não trazem breaking changes, mas incluem correções de bugs importantes (e podem incluir correções de segurança).

<div id="test-experimental-features-on-a-separate-clickhouse-server-running-the-target-version">
  ### Teste os recursos experimentais em um servidor ClickHouse separado executando a versão de destino
</div>

A compatibilidade dos recursos experimentais pode ser comprometida a qualquer momento e de qualquer forma. Se você estiver usando recursos experimentais, verifique os changelogs e considere configurar um servidor ClickHouse separado com a versão de destino instalada para testar ali o uso desses recursos.

<div id="downgrades">
  ### Downgrades
</div>

Se você fizer um upgrade e depois perceber que a nova versão não é compatível com algum recurso do qual depende, talvez seja possível fazer downgrade para uma versão recente (com menos de um ano), desde que você ainda não tenha começado a usar nenhum dos novos recursos. Depois que os novos recursos forem usados, o downgrade não funcionará.

<div id="multiple-clickhouse-server-versions-in-a-cluster">
  ### Várias versões do ClickHouse server em um cluster
</div>

Nós nos esforçamos para manter uma janela de compatibilidade de um ano (incluindo 2 versões LTS). Isso significa que quaisquer duas versões devem conseguir funcionar juntas em um cluster se a diferença entre elas for inferior a um ano (ou se houver menos de duas versões LTS entre elas). No entanto, recomenda-se atualizar todos os membros do cluster para a mesma versão o mais rápido possível, pois podem ocorrer alguns problemas menores (como lentidão em consultas distribuídas, erros recuperáveis em algumas operações em segundo plano no ReplicatedMergeTree etc.).

Nunca recomendamos executar versões diferentes no mesmo cluster quando as datas de lançamento estiverem separadas por mais de um ano. Embora não esperemos perda de dados, o cluster pode se tornar inutilizável. Os problemas que você deve esperar se houver mais de um ano de diferença entre as versões incluem:

* o cluster pode não funcionar
* algumas (ou até todas) consultas podem falhar com erros arbitrários
* erros/avisos arbitrários podem aparecer nos logs
* pode ser impossível fazer downgrade

<div id="incremental-upgrades">
  ### Upgrades incrementais
</div>

Se a diferença entre a versão atual e a versão de destino for maior que um ano, recomenda-se:

* Fazer o upgrade com indisponibilidade (parar todos os servidores, fazer o upgrade de todos os servidores e iniciar todos os servidores).
* Ou fazer o upgrade por meio de uma versão intermediária (uma versão com menos de um ano a mais em relação à versão atual).

<div id="recommended-plan">
  ## Plano recomendado
</div>

Estas são as etapas recomendadas para uma atualização do ClickHouse sem downtime:

1. Certifique-se de que suas alterações de configuração não estejam no arquivo padrão `/etc/clickhouse-server/config.xml` e sim em `/etc/clickhouse-server/config.d/`, pois `/etc/clickhouse-server/config.xml` pode ser sobrescrito durante uma atualização.
2. Leia os [changelogs](/pt-BR/whats-new/changelog/index.md) em busca de breaking changes (do lançamento de destino retrocedendo até o lançamento que você está usando atualmente).
3. Faça, antes da atualização, todas as alterações identificadas nas breaking changes que puderem ser realizadas e liste as alterações que precisarão ser feitas após a atualização.
4. Identifique uma ou mais réplicas de cada shard para manter em funcionamento enquanto as demais réplicas de cada shard são atualizadas.
5. Nas réplicas que serão atualizadas, uma de cada vez:

* desligue o servidor ClickHouse
* atualize o servidor para a versão de destino
* inicie o servidor ClickHouse
* aguarde até que as mensagens do Keeper indiquem que o sistema está estável
* continue para a próxima réplica6. Verifique se há erros no log do Keeper e no log do ClickHouse

7. Atualize as réplicas identificadas no passo 4 para a nova versão
8. Consulte a lista de alterações feitas nos passos 1 a 3 e faça as alterações que precisarem ser feitas após a atualização.

:::note
Esta mensagem de erro é esperada quando há várias versões do ClickHouse em execução em um ambiente replicado. Você deixará de vê-la quando todas as réplicas forem atualizadas para a mesma versão.

```text
MergeFromLogEntryTask: Code: 40. DB::Exception: Checksums of parts don't match:
hash of uncompressed files doesn't match. (CHECKSUM_DOESNT_MATCH)  Data after merge is not
byte-identical to data on another replicas.
```

:::

<div id="clickhouse-server-binary-upgrade-process">
  ## Processo de atualização do binário do servidor ClickHouse
</div>

Se o ClickHouse foi instalado por meio de pacotes `deb`, execute os seguintes comandos no servidor:

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

Se você instalou o ClickHouse usando algo diferente dos pacotes `deb` recomendados, use o método de atualização adequado.

:::note
Você pode atualizar vários servidores de uma vez, desde que não haja nenhum momento em que todas as réplicas de um shard fiquem offline.
:::

A atualização de uma versão mais antiga do ClickHouse para uma versão específica:

Por exemplo:

`xx.yy.a.b` é uma versão estável atual. A versão estável mais recente pode ser encontrada [aqui](https://github.com/ClickHouse/ClickHouse/releases)

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-server=xx.yy.a.b clickhouse-client=xx.yy.a.b clickhouse-common-static=xx.yy.a.b
$ sudo service clickhouse-server restart
```