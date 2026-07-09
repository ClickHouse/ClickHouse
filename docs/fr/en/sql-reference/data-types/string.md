---
description: 'Documentation du type de données String dans ClickHouse'
sidebar_label: 'String'
sidebar_position: 8
slug: /sql-reference/data-types/string
title: 'String'
doc_type: 'reference'
---

Chaînes de longueur arbitraire. La longueur n’est pas limitée. La valeur peut contenir n’importe quelle suite d’octets, y compris des octets nuls.
Le type String remplace les types VARCHAR, BLOB, CLOB et d’autres types utilisés dans d’autres SGBD.

Lors de la création de tables, il est possible de définir des paramètres numériques pour les champs de chaîne (par exemple `VARCHAR(255)`), mais ClickHouse les ignore.

Alias :

* `String` — `LONGTEXT`, `MEDIUMTEXT`, `TINYTEXT`, `TEXT`, `LONGBLOB`, `MEDIUMBLOB`, `TINYBLOB`, `BLOB`, `VARCHAR`, `CHAR`, `CHAR LARGE OBJECT`, `CHAR VARYING`, `CHARACTER LARGE OBJECT`, `CHARACTER VARYING`, `NCHAR LARGE OBJECT`, `NCHAR VARYING`, `NATIONAL CHARACTER LARGE OBJECT`, `NATIONAL CHARACTER VARYING`, `NATIONAL CHAR VARYING`, `NATIONAL CHARACTER`, `NATIONAL CHAR`, `BINARY LARGE OBJECT`, `BINARY VARYING`,

<div id="encodings">
  ## Encodages
</div>

ClickHouse n&#39;intègre pas la notion d&#39;encodage. Les chaînes peuvent contenir un ensemble arbitraire d&#39;octets, qui sont stockés et renvoyés tels quels.
Si vous devez stocker du texte, nous recommandons d&#39;utiliser l&#39;encodage UTF-8. Au minimum, si votre terminal utilise UTF-8 (comme recommandé), vous pouvez lire et écrire vos valeurs sans avoir à effectuer de conversions.
De même, certaines fonctions de manipulation des chaînes ont des variantes distinctes qui partent du principe que la chaîne contient un ensemble d&#39;octets représentant un texte encodé en UTF-8.
Par exemple, la fonction [length](/fr/sql-reference/functions/array-functions#length) calcule la longueur d&#39;une chaîne en octets, tandis que la fonction [lengthUTF8](../functions/string-functions.md#lengthUTF8) calcule la longueur de la chaîne en points de code Unicode, en supposant que la valeur est encodée en UTF-8.