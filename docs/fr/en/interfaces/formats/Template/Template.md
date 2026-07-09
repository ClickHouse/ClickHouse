---
alias: []
description: 'Documentation sur le format Template'
input_format: true
keywords: ['Template']
output_format: true
slug: /interfaces/formats/Template
title: 'Template'
doc_type: 'guide'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

Pour les cas où vous avez besoin de davantage de personnalisation que ce qu’offrent les autres formats standard,
le format `Template` permet à l’utilisateur de définir sa propre chaîne de format avec des placeholders pour les valeurs,
ainsi que de spécifier des règles d’échappement pour les données.

Il utilise les paramètres suivants :

| Paramètre                                                                                                                             | Description                                                                                                         |
| ------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| [`format_template_row`](#format_template_row)                                                                                         | Spécifie le path du fichier qui contient les chaînes de format pour les lignes.                                     |
| [`format_template_resultset`](#format_template_resultset)                                                                             | Spécifie le path du fichier qui contient les chaînes de format pour les lignes                                      |
| [`format_template_rows_between_delimiter`](#format_template_rows_between_delimiter)                                                   | Spécifie le délimiteur entre les lignes, imprimé (ou attendu) après chaque ligne sauf la dernière (`\n` par défaut) |
| `format_template_row_format`                                                                                                          | Spécifie la chaîne de format pour les lignes [intégré](#inline_specification).                                      |
| `format_template_resultset_format`                                                                                                    | Spécifie la chaîne de format du jeu de résultats [intégré](#inline_specification).                                  |
| Certains paramètres d’autres formats (par ex. `output_format_json_quote_64bit_integers` lors de l’utilisation de l’échappement `JSON` |                                                                                                                     |

<div id="settings-and-escaping-rules">
  ## Paramètres et règles d’échappement
</div>

<div id="format_template_row">
  ### format_template_row
</div>

Le paramètre `format_template_row` spécifie le chemin vers le fichier qui contient les chaînes de format pour les lignes, avec la syntaxe suivante :

```text
delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N
```

Où :

| Élément de syntaxe | Description                                                                                                                |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| `delimiter_i`      | Un délimiteur entre les valeurs (le symbole `$` peut être protégé par échappement sous la forme `$$`)                      |
| `column_i`         | Le nom ou l’index d’une colonne dont les valeurs doivent être sélectionnées ou insérées (si vide, la colonne sera ignorée) |
| `serializeAs_i`    | Une règle d’échappement pour les valeurs de la colonne.                                                                    |

Les règles d’échappement suivantes sont prises en charge :

| Règle d’échappement  | Description                                          |
| -------------------- | ---------------------------------------------------- |
| `CSV`, `JSON`, `XML` | Similaire aux formats du même nom                    |
| `Escaped`            | Similaire à `TSV`                                    |
| `Quoted`             | Similaire à `Values`                                 |
| `Raw`                | Sans échappement, similaire à `TSVRaw`               |
| `None`               | Aucune règle d’échappement - voir la note ci-dessous |

:::note
Si aucune règle d’échappement n’est indiquée, `None` sera utilisé. `XML` convient uniquement à la sortie.
:::

Prenons un exemple. Étant donné la chaîne de format suivante :

```text
Search phrase: ${s:Quoted}, count: ${c:Escaped}, ad price: $$${p:JSON};
```

Les valeurs suivantes seront affichées (si vous utilisez `SELECT`) ou attendues (si vous utilisez `INPUT`),
entre les délimiteurs de colonnes `Search phrase:`, `, count:`, `, ad price: $` et `;`, respectivement :

* `s` (avec la règle d’échappement `Quoted`)
* `c` (avec la règle d’échappement `Escaped`)
* `p` (avec la règle d’échappement `JSON`)

Par exemple :

* Lors d’un `INSERT`, la ligne ci-dessous correspond au modèle attendu et interpréterait les valeurs `bathroom interior design`, `2166`, `$3` dans les colonnes `Search phrase`, `count`, `ad price`.
* Lors d’un `SELECT`, la ligne ci-dessous constitue la sortie, en supposant que les valeurs `bathroom interior design`, `2166`, `$3` sont déjà stockées dans une table dans les colonnes `Search phrase`, `count`, `ad price`.

```yaml
Search phrase: 'bathroom interior design', count: 2166, ad price: $3;
```

<div id="format_template_rows_between_delimiter">
  ### format_template_rows_between_delimiter
</div>

Le paramètre `format_template_rows_between_delimiter` définit le délimiteur entre les lignes, affiché (ou attendu) après chaque ligne, sauf la dernière (`\n` par défaut)

<div id="format_template_resultset">
  ### format_template_resultset
</div>

Le paramètre `format_template_resultset` spécifie le chemin du fichier qui contient une chaîne de format pour le jeu de résultats.

La chaîne de format du jeu de résultats a la même syntaxe qu’une chaîne de format pour les lignes.
Elle permet de définir un préfixe, un suffixe et une façon d’afficher des informations supplémentaires, et contient les placeholders suivants à la place des noms de colonnes :

* `data` correspond aux lignes de données au format `format_template_row`, séparées par `format_template_rows_between_delimiter`. Ce placeholder doit être le premier de la chaîne de format.
* `totals` correspond à la ligne contenant les valeurs totales au format `format_template_row` (lors de l’utilisation de WITH TOTALS).
* `min` correspond à la ligne contenant les valeurs minimales au format `format_template_row` (lorsque extremes vaut 1).
* `max` correspond à la ligne contenant les valeurs maximales au format `format_template_row` (lorsque extremes vaut 1).
* `rows` correspond au nombre total de lignes en sortie.
* `rows_before_limit` correspond au nombre minimal de lignes qu’il y aurait eu sans LIMIT. N’est renvoyé que si la requête contient LIMIT. Si la requête contient GROUP BY, rows&#95;before&#95;limit&#95;at&#95;least correspond au nombre exact de lignes qu’il y aurait eu sans LIMIT.
* `time` correspond au temps d’exécution de la requête en secondes.
* `rows_read` correspond au nombre de lignes lues.
* `bytes_read` correspond au nombre d’octets (non compressés) lus.

Les placeholders `data`, `totals`, `min` et `max` ne doivent pas avoir de règle d’échappement spécifiée (ou `None` doit être spécifié explicitement). Les autres placeholders peuvent avoir n’importe quelle règle d’échappement.

:::note
Si le paramètre `format_template_resultset` est une chaîne vide, `${data}` est utilisé comme valeur par défaut.
:::

Pour les requêtes d’insertion, le format permet d’ignorer certaines colonnes ou certains champs si un préfixe ou un suffixe est utilisé (voir l’exemple).

<div id="inline_specification">
  ### Spécification intégrée
</div>

Il est souvent difficile, voire impossible, de déployer les configurations de format
(définies par `format_template_row`, `format_template_resultset`) du format Template dans un répertoire sur tous les nœuds d’un cluster.
De plus, le format peut être si simple qu’il n’est pas nécessaire de le placer dans un fichier.

Dans ces cas, `format_template_row_format` (pour `format_template_row`) et `format_template_resultset_format` (pour `format_template_resultset`) peuvent être utilisés pour définir directement la chaîne de modèle dans la requête,
plutôt que d’indiquer le chemin du fichier qui la contient.

:::note
Les règles relatives aux chaînes de format et aux séquences d’échappement sont les mêmes que pour :

* [`format_template_row`](#format_template_row) lors de l’utilisation de `format_template_row_format`.
* [`format_template_resultset`](#format_template_resultset) lors de l’utilisation de `format_template_resultset_format`.
  :::

<div id="example-usage">
  ## Exemple d’utilisation
</div>

Voyons deux exemples d’utilisation du format `Template` : d’abord pour sélectionner des données, puis pour en insérer.

<div id="selecting-data">
  ### Sélection des données
</div>

```sql title="Query"
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

```text title="/some/path/resultset.format"
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    ${data}
  </table>
  <table border="1"> <caption>Max</caption>
    ${max}
  </table>
  <b>Processed ${rows_read:XML} rows in ${time:XML} sec</b>
 </body>
</html>
```

```text title="/some/path/row.format"
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

```html title="Response"
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    <tr> <td></td> <td>8267016</td> </tr>
    <tr> <td>bathroom interior design</td> <td>2166</td> </tr>
    <tr> <td>clickhouse</td> <td>1655</td> </tr>
    <tr> <td>spring 2014 fashion</td> <td>1549</td> </tr>
    <tr> <td>freeform photos</td> <td>1480</td> </tr>
  </table>
  <table border="1"> <caption>Max</caption>
    <tr> <td></td> <td>8873898</td> </tr>
  </table>
  <b>Processed 3095973 rows in 0.1569913 sec</b>
 </body>
</html>
```

<div id="inserting-data">
  ### Insertion de données
</div>

```text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

```sql
INSERT INTO UserActivity SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
FORMAT Template
```

```text title="/some/path/resultset.format"
Some header\n${data}\nTotal rows: ${:CSV}\n
```

```text title="/some/path/row.format"
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

`PageViews`, `UserID`, `Duration` et `Sign` à l’intérieur des placeholders sont des noms de colonnes de la table. Les valeurs après `Useless field` dans les lignes et après `\nTotal rows:` dans le suffixe seront ignorées.
Tous les délimiteurs des données d’entrée doivent être strictement identiques à ceux des chaînes de format spécifiées.

<div id="inline_specification">
  ### Spécification intégrée
</div>

Vous en avez assez de formater manuellement des tableaux Markdown ? Dans cet exemple, nous allons voir comment utiliser le format `Template` et les paramètres de spécification en ligne pour accomplir une tâche simple : effectuer un `SELECT` sur les noms de certains formats ClickHouse depuis la table `system.formats`, puis les présenter sous forme de tableau Markdown. Cela peut facilement être réalisé à l’aide du format `Template` et des paramètres `format_template_row_format` et `format_template_resultset_format`.

Dans les exemples précédents, nous avons spécifié les chaînes de format du jeu de résultats et des lignes dans des fichiers distincts, dont les chemins étaient indiqués respectivement à l’aide des paramètres `format_template_resultset` et `format_template_row`. Ici, nous allons le faire en ligne, car notre modèle est trivial et se compose uniquement de quelques `|` et `-` pour créer le tableau Markdown. Nous allons spécifier notre chaîne de modèle du jeu de résultats à l’aide du paramètre `format_template_resultset_format`. Pour créer l’en-tête du tableau, nous avons ajouté `|ClickHouse Formats|\n|---|\n` avant `${data}`. Nous utilisons le paramètre `format_template_row_format` pour définir la chaîne de modèle ``|`{0:XML}`|`` pour nos lignes. Le format `Template` insérera nos lignes, selon le format indiqué, à l’emplacement du placeholder `${data}`. Dans cet exemple, nous n’avons qu’une seule colonne, mais si vous vouliez en ajouter d’autres, vous pourriez le faire en ajoutant `{1:XML}`, `{2:XML}`... etc. à votre chaîne de modèle de ligne, en choisissant la règle d’échappement appropriée. Dans cet exemple, nous avons choisi la règle d’échappement `XML`.

```sql title="Query"
WITH formats AS
(
 SELECT * FROM system.formats
 ORDER BY rand()
 LIMIT 5
)
SELECT * FROM formats
FORMAT Template
SETTINGS
 format_template_row_format='|`${0:XML}`|',
 format_template_resultset_format='|ClickHouse Formats|\n|---|\n${data}\n'
```

Regardez un peu ! Nous nous sommes évité la peine d’ajouter manuellement tous ces `|` et `-` pour créer ce tableau Markdown :

```response title="Response"
|ClickHouse Formats|
|---|
|`BSONEachRow`|
|`CustomSeparatedWithNames`|
|`Prometheus`|
|`DWARF`|
|`Avro`|
```