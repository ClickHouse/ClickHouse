---
description: 'Documentation de Files'
sidebar_label: 'Files'
slug: /sql-reference/functions/files
title: 'Files'
doc_type: 'reference'
---

<div id="file">
  ## file
</div>

Lit un fichier sous forme de chaîne de caractères et charge les données dans la colonne spécifiée. Le contenu du fichier n’est pas interprété.

Voir aussi la fonction de table [file](../table-functions/file.md).

**Syntaxe**

```sql
file(path[, default])
```

**Arguments**

* `path` — Le chemin du fichier par rapport à [user&#95;files&#95;path](../../operations/server-configuration-parameters/settings.md#user_files_path). Prend en charge les caractères génériques `*`, `**`, `?`, `{abc,def}` et `{N..M}`, où `N` et `M` sont des nombres et `'abc'`, `'def'` des chaînes de caractères.
* `default` — La valeur renvoyée si le fichier n’existe pas ou n’est pas accessible. Types de données pris en charge : [String](../data-types/string.md) et [NULL](/fr/operations/settings/formats#input_format_null_as_default).

**Exemple**

Insertion de données depuis les fichiers a.txt et b.txt dans une table, sous forme de chaînes :

```sql
INSERT INTO table SELECT file('a.txt'), file('b.txt');
```