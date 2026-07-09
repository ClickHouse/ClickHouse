---
description: 'Esta tabela contém mensagens de aviso sobre o servidor ClickHouse.'
keywords: [ 'tabela do sistema', 'avisos' ]
slug: /operations/system-tables/system_warnings
title: 'system.warnings'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

<SystemTableCloud />

<div id="description">
  ## Descrição
</div>

Esta tabela mostra avisos sobre o servidor ClickHouse.
Avisos do mesmo tipo são agrupados em um único aviso.
Por exemplo, se o número N de bancos de dados attached exceder um limite configurável T, uma única entrada contendo o valor atual N será exibida em vez de N entradas separadas.
Se o valor atual cair abaixo do limite, a entrada será removida da tabela.

A tabela pode ser configurada com estas definições:

* [max&#95;table&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_table_num_to_warn)
* [max&#95;database&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_database_num_to_warn)
* [max&#95;dictionary&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_dictionary_num_to_warn)
* [max&#95;view&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_view_num_to_warn)
* [max&#95;part&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_part_num_to_warn)
* [max&#95;pending&#95;mutations&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_pending_mutations_to_warn)
* [max&#95;pending&#95;mutations&#95;execution&#95;time&#95;to&#95;warn](/pt-BR/operations/server-configuration-parameters/settings#max_pending_mutations_execution_time_to_warn)
* [max&#95;named&#95;collection&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_named_collection_num_to_warn)
* [resource&#95;overload&#95;warnings](/pt-BR/operations/settings/server-overload#resource-overload-warnings)

<div id="columns">
  ## Colunas
</div>

* `message` ([String](../../sql-reference/data-types/string.md)) — Mensagem de alerta.
* `message_format_string` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — String de formato usada para formatar a mensagem.

<div id="example">
  ## Exemplo
</div>

```sql title="Query"
 SELECT * FROM system.warnings LIMIT 2 \G;
```

```text title="Response"
Row 1:
──────
message:               The number of active parts is more than 10.
message_format_string: The number of active parts is more than {}.

Row 2:
──────
message:               The number of attached databases is more than 2.
message_format_string: The number of attached databases is more than {}.
```