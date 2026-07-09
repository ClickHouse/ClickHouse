---
description: 'Um conjunto de dados que permanece sempre na RAM. Destina-se ao uso no lado
  direito do operador `IN`.'
sidebar_label: 'Set'
sidebar_position: 60
slug: /engines/table-engines/special/set
title: 'Motor de tabela Set'
doc_type: 'referência'
---

:::note
No ClickHouse Cloud, se o seu serviço foi criado com uma versão anterior à versão 25.4, você precisará definir a compatibilidade para pelo menos 25.4 usando `SET compatibility=25.4`.
:::

Um conjunto de dados que permanece sempre na RAM. Destina-se ao uso no lado direito do operador `IN` (consulte a seção &quot;operadores `IN`&quot;).

Você pode usar `INSERT` para inserir dados na tabela. Novos elementos serão adicionados ao conjunto de dados, enquanto duplicatas serão ignoradas.
Mas não é possível executar `SELECT` na tabela. A única forma de recuperar dados é usá-la no lado direito do operador `IN`.

Os dados ficam sempre na RAM. Para `INSERT`, os blocos de dados inseridos também são gravados no diretório de tabelas no disco. Ao iniciar o servidor, esses dados são carregados na RAM. Em outras palavras, após a reinicialização, os dados permanecem no lugar.

Em uma reinicialização forçada do servidor, o bloco de dados no disco pode ser perdido ou danificado. Neste último caso, talvez seja necessário excluir manualmente o arquivo com os dados danificados.

<div id="join-limitations-and-settings">
  ### Limitações e configurações
</div>

Ao criar uma tabela, aplicam-se as seguintes configurações:

<div id="persistent">
  #### Persistent
</div>

Desativa a persistência nos motores de tabela Set e [Join](/pt-BR/engines/table-engines/special/join).

Reduz a sobrecarga de E/S. Indicado para cenários que priorizam desempenho e não exigem persistência.

Valores possíveis:

* 1 — Ativado.
* 0 — Desativado.

Valor padrão: `1`.