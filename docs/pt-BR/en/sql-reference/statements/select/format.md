---
description: 'Documentação da cláusula FORMAT'
sidebar_label: 'FORMAT'
slug: /sql-reference/statements/select/format
title: 'Cláusula FORMAT'
doc_type: 'reference'
---

O ClickHouse oferece suporte a uma ampla variedade de [formatos de serialização](../../../interfaces/formats.md) que podem ser usados, entre outras finalidades, nos resultados de consulta. Há várias maneiras de escolher um formato para a saída de `SELECT`; uma delas é especificar `FORMAT format` ao final da consulta para obter os dados resultantes no formato desejado.

Um formato específico pode ser usado por conveniência, para integração com outros sistemas ou para melhorar o desempenho.

<div id="default-format">
  ## Formato padrão
</div>

Se a cláusula `FORMAT` for omitida, será usado o formato padrão, que depende tanto das configurações quanto da interface usada para acessar o servidor ClickHouse. Para a [interface HTTP](/pt-BR/interfaces/http) e o [cliente de linha de comando](../../../interfaces/client.md) no modo batch, o formato padrão é `TabSeparated`. Para o cliente de linha de comando no modo interativo, o formato padrão é `PrettyCompact` (ele produz tabelas compactas legíveis por pessoas).

<div id="implementation-details">
  ## Detalhes de implementação
</div>

Ao usar o cliente de linha de comando, os dados são sempre transmitidos pela rede em um formato interno eficiente (`Native`). O cliente interpreta de forma independente a cláusula `FORMAT` da consulta e formata os dados por conta própria (aliviando, assim, a rede e o servidor da carga adicional).