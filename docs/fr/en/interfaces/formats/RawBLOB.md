---
description: 'Documentation du format RawBLOB'
keywords: ['RawBLOB']
slug: /interfaces/formats/RawBLOB
title: 'RawBLOB'
doc_type: 'reference'
---

<div id="description">
  ## Description
</div>

Les formats `RawBLOB` lisent toutes les données d&#39;entrée comme une seule valeur. Seule une table comportant un unique champ de type [`String`](/fr/sql-reference/data-types/string.md) ou équivalent peut être analysée.
Le résultat est renvoyé au format binaire, sans délimiteurs ni échappement. Si plusieurs valeurs sont renvoyées, le format devient ambigu et il devient impossible de relire les données.

<div id="raw-formats-comparison">
  ### Comparaison des formats bruts
</div>

Vous trouverez ci-dessous une comparaison des formats `RawBLOB` et [`TabSeparatedRaw`](./TabSeparated/TabSeparatedRaw.md).

`RawBLOB` :

* les données sont renvoyées au format binaire, sans échappement ;
* il n’y a pas de délimiteurs entre les valeurs ;
* aucun saut de ligne à la fin de chaque valeur.

`TabSeparatedRaw` :

* les données sont renvoyées sans échappement ;
* les lignes contiennent des valeurs séparées par des tabulations ;
* un saut de ligne suit la dernière valeur de chaque ligne.

Vous trouverez ci-dessous une comparaison des formats `RawBLOB` et [RowBinary](./RowBinary/RowBinary.md).

`RawBLOB` :

* les champs String sont renvoyés sans être préfixés par leur longueur.

`RowBinary` :

* les champs String sont représentés par leur longueur au format varint ([LEB128] non signé (https://en.wikipedia.org/wiki/LEB128)), suivie des octets de la chaîne.

Lorsque des données vides sont fournies en entrée à `RawBLOB`, ClickHouse lève une exception :

```text
Code: 108. DB::Exception: No data to insert
```

<div id="example-usage">
  ## Exemple d&#39;utilisation
</div>

```bash title="Query"
$ clickhouse-client --query "CREATE TABLE {some_table} (a String) ENGINE = Memory;"
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT RawBLOB"
$ clickhouse-client --query "SELECT * FROM {some_table} FORMAT RawBLOB" | md5sum
```

```text title="Response"
f9725a22f9191e064120d718e26862a9  -
```

<div id="format-settings">
  ## Paramètres de format
</div>
