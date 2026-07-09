---
alias: []
description: 'Documentation sur le format Regexp'
input_format: true
keywords: ['Regexp']
output_format: false
slug: /interfaces/formats/Regexp
title: 'Regexp'
doc_type: 'référence'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✗      |       |

<div id="description">
  ## Description
</div>

Le format `Regex` analyse chaque ligne des données importées selon l&#39;expression régulière fournie.

**Utilisation**

L&#39;expression régulière définie par le paramètre [format&#95;regexp](/fr/operations/settings/settings-formats.md/#format_regexp) est appliquée à chaque ligne des données importées. Le nombre de sous-motifs dans l&#39;expression régulière doit être égal au nombre de colonnes du jeu de données importé.

Les lignes des données importées doivent être séparées par le caractère de nouvelle ligne `'\n'` ou par une fin de ligne au format DOS `"\r\n"`.

Le contenu de chaque sous-motif correspondant est analysé à l&#39;aide de la méthode du type de données correspondant, conformément au paramètre [format&#95;regexp&#95;escaping&#95;rule](/fr/operations/settings/settings-formats.md/#format_regexp_escaping_rule).

Si l&#39;expression régulière ne correspond pas à la ligne et que [format&#95;regexp&#95;skip&#95;unmatched](/fr/operations/settings/settings-formats.md/#format_regexp_escaping_rule) est défini sur 1, la ligne est ignorée sans avertissement. Sinon, une exception est levée.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

Prenons le fichier `data.tsv` :

```text title="data.tsv"
id: 1 array: [1,2,3] string: str1 date: 2020-01-01
id: 2 array: [1,2,3] string: str2 date: 2020-01-02
id: 3 array: [1,2,3] string: str3 date: 2020-01-03
```

et la table `imp_regex_table` :

```sql title="Query"
CREATE TABLE imp_regex_table (id UInt32, array Array(UInt32), string String, date Date) ENGINE = Memory;
```

Nous allons insérer les données du fichier mentionné précédemment dans la table ci-dessus à l’aide de la requête suivante :

```bash title="Query"
$ cat data.tsv | clickhouse-client  --query "INSERT INTO imp_regex_table SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='Escaped', format_regexp_skip_unmatched=0 FORMAT Regexp;"
```

Nous pouvons maintenant `SELECT` les données de la table pour voir comment le format `Regex` a interprété les données du fichier :

```sql title="Query"
SELECT * FROM imp_regex_table;
```

```text title="Response"
┌─id─┬─array───┬─string─┬───────date─┐
│  1 │ [1,2,3] │ str1   │ 2020-01-01 │
│  2 │ [1,2,3] │ str2   │ 2020-01-02 │
│  3 │ [1,2,3] │ str3   │ 2020-01-03 │
└────┴─────────┴────────┴────────────┘
```

<div id="format-settings">
  ## Paramètres du format
</div>

Lorsque vous utilisez le format `Regexp`, vous pouvez définir les paramètres suivants :

* `format_regexp` — [String](/fr/sql-reference/data-types/string.md). Contient une expression régulière au format [re2](https://github.com/google/re2/wiki/Syntax).

* `format_regexp_escaping_rule` — [String](/fr/sql-reference/data-types/string.md). Les règles d’échappement suivantes sont prises en charge :

  * CSV (comme pour [CSV](/fr/interfaces/formats/CSV)
  * JSON (comme pour [JSONEachRow](/fr/interfaces/formats/JSONEachRow)
  * Escaped (comme pour [TSV](/fr/interfaces/formats/TabSeparated)
  * Quoted (comme pour [Values](/fr/interfaces/formats/Values)
  * Raw (extrait les sous-motifs dans leur intégralité, sans règle d’échappement, comme pour [TSVRaw](/fr/interfaces/formats/TabSeparated)

* `format_regexp_skip_unmatched` — [UInt8](/fr/sql-reference/data-types/int-uint.md). Définit s’il faut lever une exception si l’expression `format_regexp` ne correspond pas aux données importées. Peut être défini sur `0` ou `1`.