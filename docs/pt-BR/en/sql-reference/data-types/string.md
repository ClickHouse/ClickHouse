---
description: 'Documentação para o tipo de dado String no ClickHouse'
sidebar_label: 'String'
sidebar_position: 8
slug: /sql-reference/data-types/string
title: 'String'
doc_type: 'reference'
---

String de comprimento arbitrário. O comprimento não é limitado. O valor pode conter qualquer conjunto de bytes, inclusive bytes nulos.
O tipo String substitui os tipos VARCHAR, BLOB, CLOB e outros de outros sistemas gerenciadores de banco de dados.

Ao criar tabelas, é possível definir parâmetros numéricos para campos do tipo string (por exemplo, `VARCHAR(255)`), mas o ClickHouse os ignora.

Aliases:

* `String` — `LONGTEXT`, `MEDIUMTEXT`, `TINYTEXT`, `TEXT`, `LONGBLOB`, `MEDIUMBLOB`, `TINYBLOB`, `BLOB`, `VARCHAR`, `CHAR`, `CHAR LARGE OBJECT`, `CHAR VARYING`, `CHARACTER LARGE OBJECT`, `CHARACTER VARYING`, `NCHAR LARGE OBJECT`, `NCHAR VARYING`, `NATIONAL CHARACTER LARGE OBJECT`, `NATIONAL CHARACTER VARYING`, `NATIONAL CHAR VARYING`, `NATIONAL CHARACTER`, `NATIONAL CHAR`, `BINARY LARGE OBJECT`, `BINARY VARYING`,

<div id="encodings">
  ## Codificações
</div>

O ClickHouse não tem o conceito de codificações. As strings podem conter um conjunto arbitrário de bytes, que são armazenados e exibidos exatamente como estão.
Se você precisar armazenar textos, recomendamos usar a codificação UTF-8. No mínimo, se o seu terminal usar UTF-8 (como recomendado), você poderá ler e gravar seus valores sem fazer conversões.
Da mesma forma, determinadas funções para trabalhar com strings têm variações específicas que partem do pressuposto de que a string contém um conjunto de bytes que representa um texto codificado em UTF-8.
Por exemplo, a função [length](/pt-BR/sql-reference/functions/array-functions#length) calcula o comprimento da string em bytes, enquanto a função [lengthUTF8](../functions/string-functions.md#lengthUTF8) calcula o comprimento da string em pontos de código Unicode, assumindo que o valor está codificado em UTF-8.