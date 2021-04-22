---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 21
toc_title: "Formats d'entr\xE9e et de sortie"
---

# Formats pour les données D'entrée et de sortie {#formats}

ClickHouse peut accepter et renvoyer des données dans différents formats. Un format pris en charge pour l'entrée peut être utilisée pour analyser les données fournies à `INSERT`s, pour effectuer `SELECT`s à partir d'une table sauvegardée par fichier, telle Qu'un fichier, une URL ou un HDFS, ou pour lire un dictionnaire externe. Un format pris en charge pour la sortie peut être utilisé pour organiser
les résultats d'une `SELECT` et pour effectuer `INSERT`s dans une table sauvegardée par fichier.

Les formats pris en charge sont:

| Format                                                          | Entrée | Sortie |
|-----------------------------------------------------------------|--------|--------|
| [TabSeparated](#tabseparated)                                   | ✔      | ✔      |
| [TabSeparatedRaw](#tabseparatedraw)                             | ✗      | ✔      |
| [TabSeparatedWithNames](#tabseparatedwithnames)                 | ✔      | ✔      |
| [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes) | ✔      | ✔      |
| [Modèle](#format-template)                                      | ✔      | ✔      |
| [TemplateIgnoreSpaces](#templateignorespaces)                   | ✔      | ✗      |
| [CSV](#csv)                                                     | ✔      | ✔      |
| [CSVWithNames](#csvwithnames)                                   | ✔      | ✔      |
| [CustomSeparated](#format-customseparated)                      | ✔      | ✔      |
| [Valeur](#data-format-values)                                   | ✔      | ✔      |
| [Vertical](#vertical)                                           | ✗      | ✔      |
| [VerticalRaw](#verticalraw)                                     | ✗      | ✔      |
| [JSON](#json)                                                   | ✗      | ✔      |
| [JSONCompact](#jsoncompact)                                     | ✗      | ✔      |
| [JSONEachRow](#jsoneachrow)                                     | ✔      | ✔      |
| [TSKV](#tskv)                                                   | ✔      | ✔      |
| [Joli](#pretty)                                                 | ✗      | ✔      |
| [PrettyCompact](#prettycompact)                                 | ✗      | ✔      |
| [PrettyCompactMonoBlock](#prettycompactmonoblock)               | ✗      | ✔      |
| [PrettyNoEscapes](#prettynoescapes)                             | ✗      | ✔      |
| [PrettySpace](#prettyspace)                                     | ✗      | ✔      |
| [Protobuf](#protobuf)                                           | ✔      | ✔      |
| [Avro](#data-format-avro)                                       | ✔      | ✔      |
| [AvroConfluent](#data-format-avro-confluent)                    | ✔      | ✗      |
| [Parquet](#data-format-parquet)                                 | ✔      | ✔      |
| [ORC](#data-format-orc)                                         | ✔      | ✗      |
| [RowBinary](#rowbinary)                                         | ✔      | ✔      |
| [Rowbinarywithnamesettypes](#rowbinarywithnamesandtypes)        | ✔      | ✔      |
| [Natif](#native)                                                | ✔      | ✔      |
| [NULL](#null)                                                   | ✗      | ✔      |
| [XML](#xml)                                                     | ✗      | ✔      |
| [CapnProto](#capnproto)                                         | ✔      | ✗      |

Vous pouvez contrôler certains paramètres de traitement de format avec les paramètres ClickHouse. Pour plus d'informations, lisez le [Paramètre](../operations/settings/settings.md) section.

## TabSeparated {#tabseparated}

Au format TabSeparated, les données sont écrites par ligne. Chaque ligne contient des valeurs séparées par des onglets. Chaque valeur est suivie par un onglet, à l'exception de la dernière valeur dans la ligne, qui est suivi par un saut de ligne. Les flux de ligne strictement Unix sont supposés partout. La dernière ligne doit également contenir un saut de ligne à la fin. Les valeurs sont écrites au format texte, sans guillemets et avec des caractères spéciaux échappés.

Ce format est également disponible sous le nom de `TSV`.

Le `TabSeparated` le format est pratique pour le traitement des données à l'aide de programmes et de scripts personnalisés. Il est utilisé par défaut dans L'interface HTTP et dans le mode batch du client de ligne de commande. Ce format permet également de transférer des données entre différents SGBD. Par exemple, vous pouvez obtenir un dump de MySQL et le télécharger sur ClickHouse, ou vice versa.

Le `TabSeparated` format prend en charge la sortie des valeurs totales (lors de L'utilisation avec des totaux) et des valeurs extrêmes (lorsque ‘extremes’ est réglé sur 1). Dans ces cas, les valeurs totales et les extrêmes sont sorties après les données principales. Le résultat principal, les valeurs totales et les extrêmes sont séparés les uns des autres par une ligne vide. Exemple:

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated``
```

``` text
2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

1970-01-01      8873898

2014-03-17      1031592
2014-03-23      1406958
```

### Mise En Forme Des Données {#data-formatting}

Les nombres entiers sont écrits sous forme décimale. Les numéros peuvent contenir un supplément “+” caractère au début (ignoré lors de l'analyse, et non enregistré lors du formatage). Les nombres non négatifs ne peuvent pas contenir le signe négatif. Lors de la lecture, il est permis d'analyser une chaîne vide en tant que zéro, ou (pour les types signés) une chaîne composée d'un signe moins en tant que zéro. Les nombres qui ne correspondent pas au type de données correspondant peuvent être analysés comme un nombre différent, sans message d'erreur.

Les nombres à virgule flottante sont écrits sous forme décimale. Le point est utilisé comme séparateur décimal. Les entrées exponentielles sont prises en charge, tout comme ‘inf’, ‘+inf’, ‘-inf’, et ‘nan’. Une entrée de nombres à virgule flottante peut commencer ou se terminer par une virgule décimale.
Pendant le formatage, la précision peut être perdue sur les nombres à virgule flottante.
Pendant l'analyse, il n'est pas strictement nécessaire de lire le nombre représentable de la machine le plus proche.

Les Dates sont écrites au format AAAA-MM-JJ et analysées dans le même format, mais avec tous les caractères comme séparateurs.
Les Dates avec les heures sont écrites dans le format `YYYY-MM-DD hh:mm:ss` et analysé dans le même format, mais avec des caractères comme séparateurs.
Tout cela se produit dans le fuseau horaire du système au moment où le client ou le serveur démarre (selon lequel d'entre eux formate les données). Pour les dates avec des heures, l'heure d'été n'est pas spécifiée. Donc, si un vidage a des temps pendant l'heure d'été, le vidage ne correspond pas sans équivoque aux données, et l'analyse sélectionnera l'une des deux fois.
Lors d'une opération de lecture, les dates et dates incorrectes avec des heures peuvent être analysées avec un débordement naturel ou en tant que dates et heures nulles, sans message d'erreur.

À titre d'exception, l'analyse des dates avec des heures est également prise en charge au format d'horodatage Unix, si elle se compose exactement de 10 chiffres décimaux. Le résultat n'est pas dépendant du fuseau horaire. Les formats AAAA-MM-JJ hh:mm:ss et NNNNNNNNNN sont différenciés automatiquement.

Les chaînes sont sorties avec des caractères spéciaux échappés par une barre oblique inverse. Les séquences d'échappement suivantes sont utilisées pour la sortie: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. L'analyse prend également en charge les séquences `\a`, `\v`, et `\xHH` (séquences d'échappement hexadécimales) et `\c` séquences, où `c` est un caractère (ces séquences sont convertis à `c`). Ainsi, la lecture de données prend en charge les formats où un saut de ligne peut être écrit comme `\n` ou `\` ou comme un saut de ligne. Par exemple, la chaîne `Hello world` avec un saut de ligne entre les mots au lieu de l'espace peut être analysé dans l'une des variantes suivantes:

``` text
Hello\nworld

Hello\
world
```

La deuxième variante est prise en charge car MySQL l'utilise lors de l'écriture de vidages séparés par des tabulations.

L'ensemble minimum de caractères que vous devez échapper lors du passage de données au format TabSeparated: tab, saut de ligne (LF) et barre oblique inverse.

Seul un petit ensemble de symboles sont échappés. Vous pouvez facilement tomber sur une valeur de chaîne que votre terminal va ruiner en sortie.

Les tableaux sont écrits sous la forme d'une liste de valeurs séparées par des virgules entre crochets. Le nombre d'éléments dans le tableau sont formatés comme normalement. `Date` et `DateTime` les types sont écrits entre guillemets simples. Les chaînes sont écrites entre guillemets simples avec les mêmes règles d'échappement que ci-dessus.

[NULL](../sql-reference/syntax.md) est formaté en tant qu' `\N`.

Chaque élément de [Imbriqué](../sql-reference/data-types/nested-data-structures/nested.md) structures est représenté sous forme de tableau.

Exemple:

``` sql
CREATE TABLE nestedt
(
    `id` UInt8,
    `aux` Nested(
        a UInt8,
        b String
    )
)
ENGINE = TinyLog
```

``` sql
INSERT INTO nestedt Values ( 1, [1], ['a'])
```

``` sql
SELECT * FROM nestedt FORMAT TSV
```

``` text
1  [1]    ['a']
```

## TabSeparatedRaw {#tabseparatedraw}

Diffère de `TabSeparated` format en ce que les lignes sont écrites sans échappement.
Ce format n'est approprié que pour la sortie d'un résultat de requête, mais pas pour l'analyse (récupération des données à insérer dans une table).

Ce format est également disponible sous le nom de `TSVRaw`.

## TabSeparatedWithNames {#tabseparatedwithnames}

Diffère de la `TabSeparated` formater en ce que les noms de colonne sont écrits dans la première ligne.
Pendant l'analyse, la première ligne est complètement ignorée. Vous ne pouvez pas utiliser les noms de colonnes pour déterminer leur position ou vérifier leur exactitude.
(La prise en charge de l'analyse de la ligne d'en-tête peut être ajoutée à l'avenir.)

Ce format est également disponible sous le nom de `TSVWithNames`.

## TabSeparatedWithNamesAndTypes {#tabseparatedwithnamesandtypes}

Diffère de la `TabSeparated` format que les noms de colonne sont écrits à la première ligne, tandis que les types de colonne sont dans la deuxième rangée.
Pendant l'analyse, les première et deuxième lignes sont complètement ignorées.

Ce format est également disponible sous le nom de `TSVWithNamesAndTypes`.

## Modèle {#format-template}

Ce format permet de spécifier une chaîne de format personnalisée avec des espaces réservés pour les valeurs avec une règle d'échappement spécifiée.

Il utilise les paramètres `format_template_resultset`, `format_template_row`, `format_template_rows_between_delimiter` and some settings of other formats (e.g. `output_format_json_quote_64bit_integers` lors de l'utilisation de `JSON` s'échapper, voir plus loin)

Paramètre `format_template_row` spécifie le chemin d'accès au fichier, qui contient une chaîne de format pour les lignes avec la syntaxe suivante:

`delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N`,

où `delimiter_i` est un délimiteur entre les valeurs (`$` symbole peut être échappé comme `$$`),
`column_i` est un nom ou un index d'une colonne dont les valeurs sont choisies ou inséré (si vide, alors la colonne sera ignoré),
`serializeAs_i` est une règle d'échappement pour les valeurs de colonne. Les règles d'échappement suivantes sont prises en charge:

-   `CSV`, `JSON`, `XML` (de même pour les formats des mêmes noms)
-   `Escaped` (de la même manière à `TSV`)
-   `Quoted` (de la même manière à `Values`)
-   `Raw` (sans s'échapper, de même pour `TSVRaw`)
-   `None` (pas de règle d'échappement, voir plus loin)

Si une règle d'échappement est omise, alors `None` sera utilisé. `XML` et `Raw` sont adaptés uniquement pour la sortie.

Donc, pour la chaîne de format suivante:

      `Search phrase: ${SearchPhrase:Quoted}, count: ${c:Escaped}, ad price: $$${price:JSON};`

les valeurs de `SearchPhrase`, `c` et `price` colonnes, qui sont échappées comme `Quoted`, `Escaped` et `JSON` imprimées (pour sélectionner) ou sera prévu (insert) entre `Search phrase:`, `, count:`, `, ad price: $` et `;` délimiteurs respectivement. Exemple:

`Search phrase: 'bathroom interior design', count: 2166, ad price: $3;`

Le `format_template_rows_between_delimiter` paramètre spécifie délimiteur entre les lignes, qui est imprimé (ou attendu) après chaque ligne, sauf la dernière (`\n` par défaut)

Paramètre `format_template_resultset` spécifie le chemin d'accès au fichier, qui contient une chaîne de format pour resultset. Format string for resultset a la même syntaxe qu'une chaîne de format pour row et permet de spécifier un préfixe, un suffixe et un moyen d'imprimer des informations supplémentaires. Il contient les espaces réservés suivants au lieu des noms de colonnes:

-   `data` est les lignes avec des données dans `format_template_row` format, séparés par des `format_template_rows_between_delimiter`. Cet espace doit être le premier espace réservé dans la chaîne de format.
-   `totals` est la ligne avec des valeurs totales dans `format_template_row` format (lors de L'utilisation avec des totaux)
-   `min` est la ligne avec des valeurs minimales dans `format_template_row` format (lorsque les extrêmes sont définis sur 1)
-   `max` est la ligne avec des valeurs maximales en `format_template_row` format (lorsque les extrêmes sont définis sur 1)
-   `rows` le nombre total de lignes de sortie
-   `rows_before_limit` est le nombre minimal de lignes qu'il y aurait eu sans limite. Sortie uniquement si la requête contient LIMIT. Si la requête contient GROUP BY, rows_before_limit_at_least est le nombre exact de lignes qu'il y aurait eu sans limite.
-   `time` est le temps d'exécution de la requête en secondes
-   `rows_read` est le nombre de lignes a été lu
-   `bytes_read` est le nombre d'octets (non compressé) a été lu

Réservé `data`, `totals`, `min` et `max` ne doit pas avoir de règle d'échappement spécifiée (ou `None` doit être spécifié explicitement). Les espaces réservés restants peuvent avoir une règle d'échappement spécifiée.
Si l' `format_template_resultset` paramètre est une chaîne vide, `${data}` est utilisé comme valeur par défaut.
Pour insérer des requêtes format permet de sauter certaines colonnes ou certains champs si préfixe ou suffixe (voir Exemple).

Sélectionnez exemple:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

`/some/path/resultset.format`:

``` text
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

`/some/path/row.format`:

``` text
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

Résultat:

``` html
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    <tr> <td></td> <td>8267016</td> </tr>
    <tr> <td>bathroom interior design</td> <td>2166</td> </tr>
    <tr> <td>yandex</td> <td>1655</td> </tr>
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

Insérez exemple:

``` text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

``` sql
INSERT INTO UserActivity FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
```

`/some/path/resultset.format`:

``` text
Some header\n${data}\nTotal rows: ${:CSV}\n
```

`/some/path/row.format`:

``` text
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

`PageViews`, `UserID`, `Duration` et `Sign` à l'intérieur des espaces réservés sont des noms de colonnes dans la table. Les valeurs après `Useless field` en rangées et après `\nTotal rows:` dans le suffixe sera ignoré.
Tous les séparateurs dans les données d'entrée doivent être strictement égal à délimiteurs dans les chaînes de format.

## TemplateIgnoreSpaces {#templateignorespaces}

Ce format est adapté uniquement pour l'entrée.
Semblable à `Template`, mais ignore les espaces entre les séparateurs et les valeurs dans le flux d'entrée. Toutefois, si les chaînes de format contiennent des espaces, ces caractères seront attendus dans le flux d'entrée. Permet également de spécifier des espaces réservés vides (`${}` ou `${:None}`) pour diviser un délimiteur en parties séparées pour ignorer les espaces entre eux. Ces espaces réservés sont utilisés uniquement pour ignorer les caractères d'espace.
Il est possible de lire `JSON` l'utilisation de ce format, si les valeurs des colonnes ont le même ordre dans toutes les lignes. Par exemple, la requête suivante peut être utilisée pour insérer des données à partir d'un exemple de format de sortie [JSON](#json):

``` sql
INSERT INTO table_name FORMAT TemplateIgnoreSpaces SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = ','
```

`/some/path/resultset.format`:

``` text
{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}
```

`/some/path/row.format`:

``` text
{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}
```

## TSKV {#tskv}

Similaire à TabSeparated, mais affiche une valeur au format name = value. Les noms sont échappés de la même manière qu'au format TabSeparated, et le symbole = est également échappé.

``` text
SearchPhrase=   count()=8267016
SearchPhrase=bathroom interior design    count()=2166
SearchPhrase=yandex     count()=1655
SearchPhrase=2014 spring fashion    count()=1549
SearchPhrase=freeform photos       count()=1480
SearchPhrase=angelina jolie    count()=1245
SearchPhrase=omsk       count()=1112
SearchPhrase=photos of dog breeds    count()=1091
SearchPhrase=curtain designs        count()=1064
SearchPhrase=baku       count()=1000
```

[NULL](../sql-reference/syntax.md) est formaté en tant qu' `\N`.

``` sql
SELECT * FROM t_null FORMAT TSKV
```

``` text
x=1    y=\N
```

Quand il y a un grand nombre de petites colonnes, ce format est inefficace, et il n'y a généralement pas de raison de l'utiliser. Néanmoins, ce n'est pas pire que JSONEachRow en termes d'efficacité.

Both data output and parsing are supported in this format. For parsing, any order is supported for the values of different columns. It is acceptable for some values to be omitted – they are treated as equal to their default values. In this case, zeros and blank rows are used as default values. Complex values that could be specified in the table are not supported as defaults.

L'analyse permet la présence du champ supplémentaire `tskv` sans le signe égal ou de valeur. Ce champ est ignoré.

## CSV {#csv}

Format des valeurs séparées par des virgules ([RFC](https://tools.ietf.org/html/rfc4180)).

Lors du formatage, les lignes sont entourées de guillemets doubles. Un guillemet double à l'intérieur d'une chaîne est affiché sous la forme de deux guillemets doubles dans une rangée. Il n'y a pas d'autres règles pour échapper les caractères. Date et date-heure sont entre guillemets. Les nombres sont produits sans guillemets. Les valeurs sont séparées par un caractère délimiteur, qui est `,` par défaut. Le caractère délimiteur est défini dans le paramètre [format_csv_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter). Les lignes sont séparées à L'aide du saut de ligne Unix (LF). Les tableaux sont sérialisés au format CSV comme suit: tout d'abord, le tableau est sérialisé en une chaîne comme au format TabSeparated, puis la chaîne résultante est sortie au format CSV entre guillemets doubles. Les Tuples au format CSV sont sérialisés en tant que colonnes séparées (c'est-à-dire que leur imbrication dans le tuple est perdue).

``` bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

\*Par défaut, le délimiteur est `,`. Voir la [format_csv_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter) réglage pour plus d'informations.

Lors de l'analyse, toutes les valeurs peuvent être analysés avec ou sans guillemets. Les guillemets doubles et simples sont pris en charge. Les lignes peuvent également être organisées sans guillemets. Dans ce cas, ils sont analysés jusqu'au caractère délimiteur ou au saut de ligne (CR ou LF). En violation de la RFC, lors de l'analyse des lignes sans guillemets, les espaces et les onglets de début et de fin sont ignorés. Pour le saut de ligne, les types Unix (LF), Windows (CR LF) et Mac OS Classic (CR LF) sont tous pris en charge.

Les valeurs d'entrée non cotées vides sont remplacées par des valeurs par défaut pour les colonnes respectives, si
[input_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)
est activé.

`NULL` est formaté en tant qu' `\N` ou `NULL` ou une chaîne vide non cotée (voir paramètres [input_format_csv_unquoted_null_literal_as_null](../operations/settings/settings.md#settings-input_format_csv_unquoted_null_literal_as_null) et [input_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)).

Le format CSV prend en charge la sortie des totaux et des extrêmes de la même manière que `TabSeparated`.

## CSVWithNames {#csvwithnames}

Imprime également la ligne d'en-tête, semblable à `TabSeparatedWithNames`.

## CustomSeparated {#format-customseparated}

Semblable à [Modèle](#format-template), mais il imprime ou lit toutes les colonnes et utilise la règle d'échappement du paramètre `format_custom_escaping_rule` et délimiteurs de paramètres `format_custom_field_delimiter`, `format_custom_row_before_delimiter`, `format_custom_row_after_delimiter`, `format_custom_row_between_delimiter`, `format_custom_result_before_delimiter` et `format_custom_result_after_delimiter`, pas à partir de chaînes de format.
Il y a aussi `CustomSeparatedIgnoreSpaces` le format, qui est similaire à `TemplateIgnoreSpaces`.

## JSON {#json}

Sorties de données au format JSON. Outre les tables de données, il génère également des noms et des types de colonnes, ainsi que des informations supplémentaires: le nombre total de lignes de sortie et le nombre de lignes qui auraient pu être sorties s'il n'y avait pas de limite. Exemple:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

``` json
{
        "meta":
        [
                {
                        "name": "SearchPhrase",
                        "type": "String"
                },
                {
                        "name": "c",
                        "type": "UInt64"
                }
        ],

        "data":
        [
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                },
                {
                        "SearchPhrase": "bathroom interior design",
                        "c": "2166"
                },
                {
                        "SearchPhrase": "yandex",
                        "c": "1655"
                },
                {
                        "SearchPhrase": "spring 2014 fashion",
                        "c": "1549"
                },
                {
                        "SearchPhrase": "freeform photos",
                        "c": "1480"
                }
        ],

        "totals":
        {
                "SearchPhrase": "",
                "c": "8873898"
        },

        "extremes":
        {
                "min":
                {
                        "SearchPhrase": "",
                        "c": "1480"
                },
                "max":
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                }
        },

        "rows": 5,

        "rows_before_limit_at_least": 141137
}
```

Le JSON est compatible avec JavaScript. Pour ce faire, certains caractères sont en outre échappés: la barre oblique `/` s'est échappée comme l' `\/`; sauts de ligne alternatifs `U+2028` et `U+2029`, qui cassent certains navigateurs, sont échappés comme `\uXXXX`. Les caractères de contrôle ASCII sont échappés: retour arrière, flux de formulaire, saut de ligne, retour chariot et tabulation horizontale sont remplacés par `\b`, `\f`, `\n`, `\r`, `\t` , ainsi que les octets restants dans la plage 00-1F en utilisant `\uXXXX` sequences. Invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences. For compatibility with JavaScript, Int64 and UInt64 integers are enclosed in double-quotes by default. To remove the quotes, you can set the configuration parameter [output_format_json_quote_64bit_integers](../operations/settings/settings.md#session_settings-output_format_json_quote_64bit_integers) à 0.

`rows` – The total number of output rows.

`rows_before_limit_at_least` Le nombre minimal de lignes aurait été sans limite. Sortie uniquement si la requête contient LIMIT.
Si la requête contient GROUP BY, rows_before_limit_at_least est le nombre exact de lignes qu'il y aurait eu sans limite.

`totals` – Total values (when using WITH TOTALS).

`extremes` – Extreme values (when extremes are set to 1).

Ce format n'est approprié que pour la sortie d'un résultat de requête, mais pas pour l'analyse (récupération des données à insérer dans une table).

Supports ClickHouse [NULL](../sql-reference/syntax.md) s'affiche à l'écran `null` dans la sortie JSON.

Voir aussi l' [JSONEachRow](#jsoneachrow) format.

## JSONCompact {#jsoncompact}

Diffère de JSON uniquement en ce que les lignes de données sont sorties dans des tableaux, pas dans des objets.

Exemple:

``` json
{
        "meta":
        [
                {
                        "name": "SearchPhrase",
                        "type": "String"
                },
                {
                        "name": "c",
                        "type": "UInt64"
                }
        ],

        "data":
        [
                ["", "8267016"],
                ["bathroom interior design", "2166"],
                ["yandex", "1655"],
                ["fashion trends spring 2014", "1549"],
                ["freeform photo", "1480"]
        ],

        "totals": ["","8873898"],

        "extremes":
        {
                "min": ["","1480"],
                "max": ["","8267016"]
        },

        "rows": 5,

        "rows_before_limit_at_least": 141137
}
```

Ce format n'est approprié que pour la sortie d'un résultat de requête, mais pas pour l'analyse (récupération des données à insérer dans une table).
Voir aussi l' `JSONEachRow` format.

## JSONEachRow {#jsoneachrow}

Lorsque vous utilisez ce format, ClickHouse affiche les lignes en tant qu'objets JSON séparés et délimités par des retours à la ligne, mais les données dans leur ensemble ne sont pas JSON valides.

``` json
{"SearchPhrase":"curtain designs","count()":"1064"}
{"SearchPhrase":"baku","count()":"1000"}
{"SearchPhrase":"","count()":"8267016"}
```

Lors de l'insertion des données, vous devez fournir un objet JSON distinct pour chaque ligne.

### Insertion De Données {#inserting-data}

``` sql
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

Clickhouse permet:

-   Toute commande de paires clé-valeur dans l'objet.
-   Omettre certaines valeurs.

ClickHouse ignore les espaces entre les éléments et les virgules après les objets. Vous pouvez passer tous les objets en une seule ligne. Vous n'avez pas à les séparer avec des sauts de ligne.

**Valeurs omises traitement**

Clickhouse remplace les valeurs omises par les valeurs par défaut pour le [types de données](../sql-reference/data-types/index.md).

Si `DEFAULT expr` clickhouse utilise différentes règles de substitution en fonction de [input_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields) paramètre.

Considérons le tableau suivant:

``` sql
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

-   Si `input_format_defaults_for_omitted_fields = 0`, alors la valeur par défaut pour `x` et `a` égal `0` (la valeur par défaut pour le `UInt32` type de données).
-   Si `input_format_defaults_for_omitted_fields = 1`, alors la valeur par défaut pour `x` égal `0` mais la valeur par défaut de `a` égal `x * 2`.

!!! note "Avertissement"
    Lors de l'insertion de données avec `insert_sample_with_metadata = 1`, ClickHouse consomme plus de ressources de calcul, par rapport à l'insertion avec `insert_sample_with_metadata = 0`.

### La Sélection De Données {#selecting-data}

Envisager l' `UserActivity` table comme un exemple:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Requête `SELECT * FROM UserActivity FORMAT JSONEachRow` retourner:

``` text
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

Contrairement à l' [JSON](#json) format, il n'y a pas de substitution de séquences UTF-8 non valides. Les valeurs sont échappés de la même manière que pour `JSON`.

!!! note "Note"
    Tout ensemble d'octets peut être sortie dans les cordes. L'utilisation de la `JSONEachRow` formater si vous êtes sûr que les données de la table peuvent être formatées en tant que JSON sans perdre aucune information.

### Utilisation de Structures imbriquées {#jsoneachrow-nested}

Si vous avez une table avec [Imbriqué](../sql-reference/data-types/nested-data-structures/nested.md) colonnes de type de données, vous pouvez insérer des données JSON avec la même structure. Activer cette fonctionnalité avec le [input_format_import_nested_json](../operations/settings/settings.md#settings-input_format_import_nested_json) paramètre.

Par exemple, considérez le tableau suivant:

``` sql
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

Comme vous pouvez le voir dans la `Nested` description du type de données, ClickHouse traite chaque composant de la structure imbriquée comme une colonne distincte (`n.s` et `n.i` pour notre table). Vous pouvez insérer des données de la manière suivante:

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

Pour insérer des données en tant qu'objet JSON hiérarchique, définissez [input_format_import_nested_json=1](../operations/settings/settings.md#settings-input_format_import_nested_json).

``` json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

Sans ce paramètre, ClickHouse lance une exception.

``` sql
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

``` text
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

``` text
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

``` sql
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

``` text
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

## Natif {#native}

Le format le plus efficace. Les données sont écrites et lues par des blocs au format binaire. Pour chaque bloc, le nombre de lignes, le nombre de colonnes, les noms et types de colonnes et les parties de colonnes de ce bloc sont enregistrés les uns après les autres. En d'autres termes, ce format est “columnar” – it doesn't convert columns to rows. This is the format used in the native interface for interaction between servers, for using the command-line client, and for C++ clients.

Vous pouvez utiliser ce format pour générer rapidement des vidages qui ne peuvent être lus que par le SGBD ClickHouse. Cela n'a pas de sens de travailler avec ce format vous-même.

## NULL {#null}

Rien n'est sortie. Cependant, la requête est traitée et, lors de l'utilisation du client de ligne de commande, les données sont transmises au client. Ceci est utilisé pour les tests, y compris les tests de performance.
Évidemment, ce format n'est approprié que pour la sortie, pas pour l'analyse.

## Joli {#pretty}

Affiche les données sous forme de tables Unicode-art, en utilisant également des séquences d'échappement ANSI pour définir les couleurs dans le terminal.
Une grille complète de la table est dessinée, et chaque ligne occupe deux lignes dans le terminal.
Chaque bloc de résultat est sorti sous la forme d'une table séparée. Ceci est nécessaire pour que les blocs puissent être sortis sans résultats de mise en mémoire tampon (la mise en mémoire tampon serait nécessaire pour pré-calculer la largeur visible de toutes les valeurs).

[NULL](../sql-reference/syntax.md) est sortie `ᴺᵁᴸᴸ`.

Exemple (montré pour le [PrettyCompact](#prettycompact) format):

``` sql
SELECT * FROM t_null
```

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Les lignes ne sont pas échappées dans les formats Pretty\*. Exemple est montré pour la [PrettyCompact](#prettycompact) format:

``` sql
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

``` text
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

Pour éviter de déverser trop de données sur le terminal, seules les 10 000 premières lignes sont imprimées. Si le nombre de lignes est supérieur ou égal à 10 000, le message “Showed first 10 000” est imprimé.
Ce format n'est approprié que pour la sortie d'un résultat de requête, mais pas pour l'analyse (récupération des données à insérer dans une table).

Le joli format prend en charge la sortie des valeurs totales (lors de L'utilisation avec des totaux) et des extrêmes (lorsque ‘extremes’ est réglé sur 1). Dans ces cas, les valeurs totales et les valeurs extrêmes sont sorties après les données principales, dans des tableaux séparés. Exemple (montré pour le [PrettyCompact](#prettycompact) format):

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT PrettyCompact
```

``` text
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

## PrettyCompact {#prettycompact}

Diffère de [Joli](#pretty) en ce que la grille est dessinée entre les lignes et le résultat est plus compact.
Ce format est utilisé par défaut dans le client de ligne de commande en mode interactif.

## PrettyCompactMonoBlock {#prettycompactmonoblock}

Diffère de [PrettyCompact](#prettycompact) dans ce cas, jusqu'à 10 000 lignes sont mises en mémoire tampon, puis sorties en tant que table unique, pas par blocs.

## PrettyNoEscapes {#prettynoescapes}

Diffère de Pretty en ce que les séquences d'échappement ANSI ne sont pas utilisées. Ceci est nécessaire pour afficher ce format dans un navigateur, ainsi que pour utiliser le ‘watch’ utilitaire de ligne de commande.

Exemple:

``` bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

Vous pouvez utiliser L'interface HTTP pour afficher dans le navigateur.

### Joliscompactnoescapes {#prettycompactnoescapes}

Le même que le réglage précédent.

### PrettySpaceNoEscapes {#prettyspacenoescapes}

Le même que le réglage précédent.

## PrettySpace {#prettyspace}

Diffère de [PrettyCompact](#prettycompact) dans cet espace (caractères d'espace) est utilisé à la place de la grille.

## RowBinary {#rowbinary}

Formats et analyse les données par ligne au format binaire. Les lignes et les valeurs sont répertoriées consécutivement, sans séparateurs.
Ce format est moins efficace que le format natif car il est basé sur des lignes.

Les entiers utilisent une représentation little-endian de longueur fixe. Par exemple, UInt64 utilise 8 octets.
DateTime est représenté par UInt32 contenant L'horodatage Unix comme valeur.
Date est représenté comme un objet UInt16 qui contient le nombre de jours depuis 1970-01-01 comme valeur.
La chaîne est représentée par une longueur varint (non signée [LEB128](https://en.wikipedia.org/wiki/LEB128)), suivie par les octets de la chaîne.
FixedString est représenté simplement comme une séquence d'octets.

Le tableau est représenté sous la forme d'une longueur varint (non signée [LEB128](https://en.wikipedia.org/wiki/LEB128)), suivie par les éléments de la matrice.

Pour [NULL](../sql-reference/syntax.md#null-literal) un soutien, un octet supplémentaire contenant 1 ou 0 est ajouté avant chaque [Nullable](../sql-reference/data-types/nullable.md) valeur. Si la valeur est 1, alors la valeur est `NULL` et cet octet est interprétée comme une valeur distincte. Si 0, la valeur après l'octet n'est pas `NULL`.

## Rowbinarywithnamesettypes {#rowbinarywithnamesandtypes}

Semblable à [RowBinary](#rowbinary) mais avec l'ajout de l'en-tête:

-   [LEB128](https://en.wikipedia.org/wiki/LEB128)- nombre codé de colonnes (N)
-   N `String`s spécification des noms de colonnes
-   N `String`s spécification des types de colonnes

## Valeur {#data-format-values}

Imprime chaque ligne entre parenthèses. Les lignes sont séparées par des virgules. Il n'y a pas de virgule après la dernière ligne. Les valeurs entre parenthèses sont également séparées par des virgules. Les nombres sont produits dans un format décimal sans guillemets. Les tableaux sont affichés entre crochets. Les chaînes, les dates et les dates avec des heures sont affichées entre guillemets. Les règles d'échappement et l'analyse sont similaires à [TabSeparated](#tabseparated) format. Pendant le formatage, les espaces supplémentaires ne sont pas insérés, mais pendant l'analyse, ils sont autorisés et ignorés (sauf pour les espaces à l'intérieur des valeurs de tableau, qui ne sont pas autorisés). [NULL](../sql-reference/syntax.md) est représentée comme `NULL`.

The minimum set of characters that you need to escape when passing data in Values ​​format: single quotes and backslashes.

C'est le format qui est utilisé dans `INSERT INTO t VALUES ...`, mais vous pouvez également l'utiliser pour le formatage des résultats de requête.

Voir aussi: [input_format_values_interpret_expressions](../operations/settings/settings.md#settings-input_format_values_interpret_expressions) et [input_format_values_deduce_templates_of_expressions](../operations/settings/settings.md#settings-input_format_values_deduce_templates_of_expressions) paramètre.

## Vertical {#vertical}

Imprime chaque valeur sur une ligne distincte avec le nom de colonne spécifié. Ce format est pratique pour imprimer une ou plusieurs lignes si chaque ligne est constituée d'un grand nombre de colonnes.

[NULL](../sql-reference/syntax.md) est sortie `ᴺᵁᴸᴸ`.

Exemple:

``` sql
SELECT * FROM t_null FORMAT Vertical
```

``` text
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

Les lignes ne sont pas échappées au format Vertical:

``` sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

``` text
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

Ce format n'est approprié que pour la sortie d'un résultat de requête, mais pas pour l'analyse (récupération des données à insérer dans une table).

## VerticalRaw {#verticalraw}

Semblable à [Vertical](#vertical), mais avec échapper désactivé. Ce format ne convient que pour la sortie des résultats de requête, pas pour l'analyse (recevoir des données et les insérer dans la table).

## XML {#xml}

Le format XML ne convient que pour la sortie, pas pour l'analyse. Exemple:

``` xml
<?xml version='1.0' encoding='UTF-8' ?>
<result>
        <meta>
                <columns>
                        <column>
                                <name>SearchPhrase</name>
                                <type>String</type>
                        </column>
                        <column>
                                <name>count()</name>
                                <type>UInt64</type>
                        </column>
                </columns>
        </meta>
        <data>
                <row>
                        <SearchPhrase></SearchPhrase>
                        <field>8267016</field>
                </row>
                <row>
                        <SearchPhrase>bathroom interior design</SearchPhrase>
                        <field>2166</field>
                </row>
                <row>
                        <SearchPhrase>yandex</SearchPhrase>
                        <field>1655</field>
                </row>
                <row>
                        <SearchPhrase>2014 spring fashion</SearchPhrase>
                        <field>1549</field>
                </row>
                <row>
                        <SearchPhrase>freeform photos</SearchPhrase>
                        <field>1480</field>
                </row>
                <row>
                        <SearchPhrase>angelina jolie</SearchPhrase>
                        <field>1245</field>
                </row>
                <row>
                        <SearchPhrase>omsk</SearchPhrase>
                        <field>1112</field>
                </row>
                <row>
                        <SearchPhrase>photos of dog breeds</SearchPhrase>
                        <field>1091</field>
                </row>
                <row>
                        <SearchPhrase>curtain designs</SearchPhrase>
                        <field>1064</field>
                </row>
                <row>
                        <SearchPhrase>baku</SearchPhrase>
                        <field>1000</field>
                </row>
        </data>
        <rows>10</rows>
        <rows_before_limit_at_least>141137</rows_before_limit_at_least>
</result>
```

Si le nom de colonne n'a pas un format acceptable, juste ‘field’ est utilisé comme le nom de l'élément. En général, la structure XML suit la structure JSON.
Just as for JSON, invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences.

Dans les valeurs de chaîne, les caractères `<` et `&` sont échappés comme `<` et `&`.

Les tableaux sont produits comme `<array><elem>Hello</elem><elem>World</elem>...</array>`,et n-uplets d' `<tuple><elem>Hello</elem><elem>World</elem>...</tuple>`.

## CapnProto {#capnproto}

Cap'n Proto est un format de message binaire similaire aux tampons de protocole et Thrift, mais pas comme JSON ou MessagePack.

Les messages Cap'n Proto sont strictement typés et ne sont pas auto-descriptifs, ce qui signifie qu'ils ont besoin d'une description de schéma externe. Le schéma est appliqué à la volée et mise en cache pour chaque requête.

``` bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits FORMAT CapnProto SETTINGS format_schema='schema:Message'"
```

Où `schema.capnp` ressemble à ceci:

``` capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

La désérialisation est efficace et n'augmente généralement pas la charge du système.

Voir aussi [Schéma De Format](#formatschema).

## Protobuf {#protobuf}

Protobuf-est un [Protocol Buffers](https://developers.google.com/protocol-buffers/) format.

Ce format nécessite un schéma de format externe. Le schéma est mis en cache entre les requêtes.
Clickhouse prend en charge les deux `proto2` et `proto3` syntaxe. Les champs répétés/optionnels/obligatoires sont pris en charge.

Exemples d'utilisation:

``` sql
SELECT * FROM test.table FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

``` bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT Protobuf SETTINGS format_schema='schemafile:MessageType'"
```

où le fichier `schemafile.proto` ressemble à ceci:

``` capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```

Pour trouver la correspondance entre les colonnes de table et les champs du type de message des tampons de protocole, ClickHouse compare leurs noms.
Cette comparaison est insensible à la casse et les caractères `_` (trait de soulignement) et `.` (dot) sont considérés comme égaux.
Si les types d'une colonne et d'un champ de message des tampons de protocole sont différents, la conversion nécessaire est appliquée.

Les messages imbriqués sont pris en charge. Par exemple, pour le champ `z` dans le type de message suivant

``` capnp
message MessageType {
  message XType {
    message YType {
      int32 z;
    };
    repeated YType y;
  };
  XType x;
};
```

ClickHouse tente de trouver une colonne nommée `x.y.z` (ou `x_y_z` ou `X.y_Z` et ainsi de suite).
Les messages imbriqués conviennent à l'entrée ou à la sortie d'un [structures de données imbriquées](../sql-reference/data-types/nested-data-structures/nested.md).

Valeurs par défaut définies dans un schéma protobuf comme ceci

``` capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

ne sont pas appliquées; la [valeurs par défaut de la table](../sql-reference/statements/create.md#create-default-values) sont utilisés à leur place.

Clickhouse entrées et sorties messages protobuf dans le `length-delimited` format.
Cela signifie avant que chaque message devrait être écrit sa longueur comme un [varint](https://developers.google.com/protocol-buffers/docs/encoding#varints).
Voir aussi [comment lire / écrire des messages protobuf délimités par la longueur dans les langues populaires](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

## Avro {#data-format-avro}

[Apache Avro](http://avro.apache.org/) est un cadre de sérialisation de données orienté ligne développé dans le projet Hadoop D'Apache.

ClickHouse Avro format prend en charge la lecture et l'écriture [Fichiers de données Avro](http://avro.apache.org/docs/current/spec.html#Object+Container+Files).

### Types De Données Correspondant {#data_types-matching}

Le tableau ci-dessous montre les types de données pris en charge et comment ils correspondent à ClickHouse [types de données](../sql-reference/data-types/index.md) dans `INSERT` et `SELECT` requête.

| Type de données Avro `INSERT`               | Type de données ClickHouse                                                                                          | Type de données Avro `SELECT` |
|---------------------------------------------|---------------------------------------------------------------------------------------------------------------------|-------------------------------|
| `boolean`, `int`, `long`, `float`, `double` | [Int (8/16/32)](../sql-reference/data-types/int-uint.md), [UInt (8/16/32)](../sql-reference/data-types/int-uint.md) | `int`                         |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](../sql-reference/data-types/int-uint.md), [UInt64](../sql-reference/data-types/int-uint.md)                 | `long`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](../sql-reference/data-types/float.md)                                                                     | `float`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](../sql-reference/data-types/float.md)                                                                     | `double`                      |
| `bytes`, `string`, `fixed`, `enum`          | [Chaîne](../sql-reference/data-types/string.md)                                                                     | `bytes`                       |
| `bytes`, `string`, `fixed`                  | [FixedString (N)](../sql-reference/data-types/fixedstring.md)                                                       | `fixed(N)`                    |
| `enum`                                      | [Enum (8/16)](../sql-reference/data-types/enum.md)                                                                  | `enum`                        |
| `array(T)`                                  | [Array(T)](../sql-reference/data-types/array.md)                                                                    | `array(T)`                    |
| `union(null, T)`, `union(T, null)`          | [Nullable (T)](../sql-reference/data-types/date.md)                                                                 | `union(null, T)`              |
| `null`                                      | [Les Valeurs Null(Nothing)](../sql-reference/data-types/special-data-types/nothing.md)                              | `null`                        |
| `int (date)` \*                             | [Date](../sql-reference/data-types/date.md)                                                                         | `int (date)` \*               |
| `long (timestamp-millis)` \*                | [DateTime64 (3)](../sql-reference/data-types/datetime.md)                                                           | `long (timestamp-millis)` \*  |
| `long (timestamp-micros)` \*                | [DateTime64 (6)](../sql-reference/data-types/datetime.md)                                                           | `long (timestamp-micros)` \*  |

\* [Types logiques Avro](http://avro.apache.org/docs/current/spec.html#Logical+Types)

Types de données Avro non pris en charge: `record` (non-root), `map`

Types de données logiques Avro non pris en charge: `uuid`, `time-millis`, `time-micros`, `duration`

### Insertion De Données {#inserting-data-1}

Pour insérer des données d'un fichier Avro dans la table ClickHouse:

``` bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

Le schéma racine du fichier Avro d'entrée doit être de `record` type.

Pour trouver la correspondance entre les colonnes de table et les champs du schéma Avro ClickHouse compare leurs noms. Cette comparaison est sensible à la casse.
Les champs inutilisés sont ignorés.

Les types de données des colonnes de la table ClickHouse peuvent différer des champs correspondants des données Avro insérées. Lors de l'insertion de données, ClickHouse interprète les types de données selon le tableau ci-dessus, puis [jeter](../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) les données au type de colonne correspondant.

### La Sélection De Données {#selecting-data-1}

Pour sélectionner des données de la table ClickHouse dans un fichier Avro:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

Les noms de colonnes doivent:

-   commencer avec `[A-Za-z_]`
-   par la suite contenir uniquement `[A-Za-z0-9_]`

Sortie Avro fichier de compression et sync intervalle peut être configuré avec [output_format_avro_codec](../operations/settings/settings.md#settings-output_format_avro_codec) et [output_format_avro_sync_interval](../operations/settings/settings.md#settings-output_format_avro_sync_interval) respectivement.

## AvroConfluent {#data-format-avro-confluent}

Avroconfluent prend en charge le décodage des messages Avro à objet unique couramment utilisés avec [Kafka](https://kafka.apache.org/) et [Confluentes Schéma De Registre](https://docs.confluent.io/current/schema-registry/index.html).

Chaque message Avro intègre un id de schéma qui peut être résolu dans le schéma réel à l'aide du registre de schéma.

Les schémas sont mis en cache une fois résolus.

L'URL du registre de schéma est configurée avec [format_avro_schema_registry_url](../operations/settings/settings.md#settings-format_avro_schema_registry_url)

### Types De Données Correspondant {#data_types-matching-1}

Même que [Avro](#data-format-avro)

### Utilisation {#usage}

Pour vérifier rapidement la résolution du schéma, vous pouvez utiliser [kafkacat](https://github.com/edenhill/kafkacat) avec [clickhouse-local](../operations/utilities/clickhouse-local.md):

``` bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```

Utiliser `AvroConfluent` avec [Kafka](../engines/table-engines/integrations/kafka.md):

``` sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent';

SET format_avro_schema_registry_url = 'http://schema-registry';

SELECT * FROM topic1_stream;
```

!!! note "Avertissement"
    Paramètre `format_avro_schema_registry_url` doit être configuré dans `users.xml` afin de maintenir sa valeur après un redémarrage.

## Parquet {#data-format-parquet}

[Apache Parquet](http://parquet.apache.org/) est un format de stockage colonnaire répandu dans L'écosystème Hadoop. ClickHouse prend en charge les opérations de lecture et d'écriture pour ce format.

### Types De Données Correspondant {#data_types-matching-2}

Le tableau ci-dessous montre les types de données pris en charge et comment ils correspondent à ClickHouse [types de données](../sql-reference/data-types/index.md) dans `INSERT` et `SELECT` requête.

| Type de données Parquet (`INSERT`) | Type de données ClickHouse                                | Type de données Parquet (`SELECT`) |
|------------------------------------|-----------------------------------------------------------|------------------------------------|
| `UINT8`, `BOOL`                    | [UInt8](../sql-reference/data-types/int-uint.md)          | `UINT8`                            |
| `INT8`                             | [Int8](../sql-reference/data-types/int-uint.md)           | `INT8`                             |
| `UINT16`                           | [UInt16](../sql-reference/data-types/int-uint.md)         | `UINT16`                           |
| `INT16`                            | [Int16](../sql-reference/data-types/int-uint.md)          | `INT16`                            |
| `UINT32`                           | [UInt32](../sql-reference/data-types/int-uint.md)         | `UINT32`                           |
| `INT32`                            | [Int32](../sql-reference/data-types/int-uint.md)          | `INT32`                            |
| `UINT64`                           | [UInt64](../sql-reference/data-types/int-uint.md)         | `UINT64`                           |
| `INT64`                            | [Int64](../sql-reference/data-types/int-uint.md)          | `INT64`                            |
| `FLOAT`, `HALF_FLOAT`              | [Float32](../sql-reference/data-types/float.md)           | `FLOAT`                            |
| `DOUBLE`                           | [Float64](../sql-reference/data-types/float.md)           | `DOUBLE`                           |
| `DATE32`                           | [Date](../sql-reference/data-types/date.md)               | `UINT16`                           |
| `DATE64`, `TIMESTAMP`              | [DateTime](../sql-reference/data-types/datetime.md)       | `UINT32`                           |
| `STRING`, `BINARY`                 | [Chaîne](../sql-reference/data-types/string.md)           | `STRING`                           |
| —                                  | [FixedString](../sql-reference/data-types/fixedstring.md) | `STRING`                           |
| `DECIMAL`                          | [Décimal](../sql-reference/data-types/decimal.md)         | `DECIMAL`                          |

Clickhouse prend en charge la précision configurable de `Decimal` type. Le `INSERT` requête traite le Parquet `DECIMAL` tapez comme le ClickHouse `Decimal128` type.

Types de données Parquet non pris en charge: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

Les types de données des colonnes de table ClickHouse peuvent différer des champs correspondants des données de Parquet insérées. Lors de l'insertion de données, ClickHouse interprète les types de données selon le tableau ci-dessus, puis [jeter](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) les données de ce type de données qui est défini pour la colonne de la table ClickHouse.

### Insertion et sélection de données {#inserting-and-selecting-data}

Vous pouvez insérer des données Parquet à partir d'un fichier dans la table ClickHouse par la commande suivante:

``` bash
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT Parquet"
```

Vous pouvez sélectionner des données à partir d'une table de ClickHouse et les enregistrer dans un fichier au format Parquet par la commande suivante:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Parquet" > {some_file.pq}
```

Pour échanger des données avec Hadoop, vous pouvez utiliser [Moteur de table HDFS](../engines/table-engines/integrations/hdfs.md).

## ORC {#data-format-orc}

[Apache ORC](https://orc.apache.org/) est un format de stockage colonnaire répandu dans L'écosystème Hadoop. Vous ne pouvez insérer des données dans ce format à ClickHouse.

### Types De Données Correspondant {#data_types-matching-3}

Le tableau ci-dessous montre les types de données pris en charge et comment ils correspondent à ClickHouse [types de données](../sql-reference/data-types/index.md) dans `INSERT` requête.

| Type de données ORC (`INSERT`) | Type de données ClickHouse                          |
|--------------------------------|-----------------------------------------------------|
| `UINT8`, `BOOL`                | [UInt8](../sql-reference/data-types/int-uint.md)    |
| `INT8`                         | [Int8](../sql-reference/data-types/int-uint.md)     |
| `UINT16`                       | [UInt16](../sql-reference/data-types/int-uint.md)   |
| `INT16`                        | [Int16](../sql-reference/data-types/int-uint.md)    |
| `UINT32`                       | [UInt32](../sql-reference/data-types/int-uint.md)   |
| `INT32`                        | [Int32](../sql-reference/data-types/int-uint.md)    |
| `UINT64`                       | [UInt64](../sql-reference/data-types/int-uint.md)   |
| `INT64`                        | [Int64](../sql-reference/data-types/int-uint.md)    |
| `FLOAT`, `HALF_FLOAT`          | [Float32](../sql-reference/data-types/float.md)     |
| `DOUBLE`                       | [Float64](../sql-reference/data-types/float.md)     |
| `DATE32`                       | [Date](../sql-reference/data-types/date.md)         |
| `DATE64`, `TIMESTAMP`          | [DateTime](../sql-reference/data-types/datetime.md) |
| `STRING`, `BINARY`             | [Chaîne](../sql-reference/data-types/string.md)     |
| `DECIMAL`                      | [Décimal](../sql-reference/data-types/decimal.md)   |

Clickhouse prend en charge la précision configurable de la `Decimal` type. Le `INSERT` requête traite de l'ORC `DECIMAL` tapez comme le ClickHouse `Decimal128` type.

Types de données ORC non pris en charge: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

Les types de données des colonnes de la table ClickHouse ne doivent pas correspondre aux champs de données Orc correspondants. Lors de l'insertion de données, ClickHouse interprète les types de données selon le tableau ci-dessus, puis [jeter](../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) les données du type de données défini pour la colonne clickhouse table.

### Insertion De Données {#inserting-data-2}

Vous pouvez insérer des données ORC à partir d'un fichier dans la table ClickHouse par la commande suivante:

``` bash
$ cat filename.orc | clickhouse-client --query="INSERT INTO some_table FORMAT ORC"
```

Pour échanger des données avec Hadoop, vous pouvez utiliser [Moteur de table HDFS](../engines/table-engines/integrations/hdfs.md).

## Schéma De Format {#formatschema}

Le nom du fichier contenant le schéma de format est défini par le paramètre `format_schema`.
Il est nécessaire de définir ce paramètre lorsqu'il est utilisé dans l'un des formats `Cap'n Proto` et `Protobuf`.
Le format de schéma est une combinaison d'un nom de fichier et le nom d'un type de message dans ce fichier, délimité par une virgule,
e.g. `schemafile.proto:MessageType`.
Si le fichier possède l'extension standard pour le format (par exemple, `.proto` pour `Protobuf`),
il peut être omis et dans ce cas, le format de schéma ressemble `schemafile:MessageType`.

Si vous entrez ou sortez des données via le [client](../interfaces/cli.md) dans le [mode interactif](../interfaces/cli.md#cli_usage) le nom de fichier spécifié dans le format de schéma
peut contenir un chemin absolu, soit un chemin relatif au répertoire courant sur le client.
Si vous utilisez le client dans le [mode batch](../interfaces/cli.md#cli_usage), le chemin d'accès au schéma doit être relatif pour des raisons de sécurité.

Si vous entrez ou sortez des données via le [Interface HTTP](../interfaces/http.md) le nom de fichier spécifié dans le format de schéma
doit être situé dans le répertoire spécifié dans [format_schema_path](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-format_schema_path)
dans la configuration du serveur.

## Sauter Les Erreurs {#skippingerrors}

Certains formats tels que `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` et `Protobuf` pouvez ignorer brisé ligne si erreur d'analyse s'est produite et poursuivre l'analyse à partir du début de la ligne suivante. Voir [input_format_allow_errors_num](../operations/settings/settings.md#settings-input_format_allow_errors_num) et
[input_format_allow_errors_ratio](../operations/settings/settings.md#settings-input_format_allow_errors_ratio) paramètre.
Limitation:
- En cas d'erreur d'analyse `JSONEachRow` ignore toutes les données jusqu'à la nouvelle ligne (ou EOF), donc les lignes doivent être délimitées par `\n` pour compter les erreurs correctement.
- `Template` et `CustomSeparated` utilisez delimiter après la dernière colonne et delimiter entre les lignes pour trouver le début de la ligne suivante, donc sauter les erreurs ne fonctionne que si au moins l'une d'entre elles n'est pas vide.

[Article Original](https://clickhouse.tech/docs/en/interfaces/formats/) <!--hide-->
