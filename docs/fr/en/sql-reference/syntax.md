---
description: 'Documentation sur la syntaxe'
sidebar_label: 'Syntaxe'
sidebar_position: 2
slug: /sql-reference/syntax
title: 'Syntaxe'
doc_type: 'reference'
---

Dans cette section, nous allons examiner la syntaxe SQL de ClickHouse.
ClickHouse utilise une syntaxe basée sur SQL, mais propose également un certain nombre d’extensions et d’optimisations.

<div id="query-parsing">
  ## Analyse syntaxique des requêtes
</div>

Il existe deux types de parseurs dans ClickHouse :

* *Un parseur SQL complet* (un parseur à descente récursive).
* *Un parseur de format de données* (un parseur rapide en flux).

Le parseur SQL complet est utilisé dans tous les cas, sauf pour la requête `INSERT`, qui fait appel aux deux parseurs.

Examinons la requête ci-dessous :

```sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

Comme cela a déjà été mentionné, la requête `INSERT` utilise les deux parseurs.
Le fragment `INSERT INTO t VALUES` est analysé par le parseur complet,
et les données `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` sont analysées par le parseur de format de données, ou parseur de flux rapide.

<details>
  <summary>Activer le parseur complet</summary>

  Vous pouvez également activer le parseur complet pour les données
  en utilisant le paramètre [`input_format_values_interpret_expressions`](../operations/settings/settings-formats.md#input_format_values_interpret_expressions).

  Lorsque le paramètre mentionné ci-dessus est défini sur `1`,
  ClickHouse essaie d’abord d’analyser les valeurs avec le parseur de flux rapide.
  En cas d’échec, ClickHouse tente d’utiliser le parseur complet pour les données, en les traitant comme une [expression](#expressions) SQL.
</details>

Les données peuvent être dans n’importe quel format.
Lorsqu’une requête est reçue, le serveur ne traite en RAM qu’au plus [max&#95;query&#95;size](../operations/settings/settings.md#max_query_size) octets de la requête
(par défaut, 1 MB), et le reste est analysé en flux.
Cela permet d’éviter les problèmes liés aux requêtes `INSERT` volumineuses, qui constituent la méthode recommandée pour insérer des données dans ClickHouse.

Lors de l’utilisation du format [`Values`](/fr/interfaces/formats/Values) dans une requête `INSERT`,
il peut sembler que les données soient analysées de la même manière que les expressions dans une requête `SELECT`, mais ce n’est pas le cas.
Le format `Values` est bien plus limité.

Le reste de cette section est consacré au parseur complet.

:::note
Pour plus d’informations sur les parseurs de format, consultez la section [Formats](../interfaces/formats.md).
:::

<div id="spaces">
  ## Espaces
</div>

* Il peut y avoir un nombre quelconque de caractères d’espacement entre les constructions syntaxiques (y compris au début et à la fin d’une requête).
* Les caractères d’espacement incluent l’espace, la tabulation, le saut de ligne, le retour chariot (CR) et le saut de page.

<div id="comments">
  ## Commentaires
</div>

ClickHouse prend en charge les commentaires de style SQL et de style C :

* Les commentaires de style SQL commencent par `--`, `#!` ou `# ` et se poursuivent jusqu’à la fin de la ligne. L’espace après `--` et `#!` peut être omis.
* Commentaires de style C :
  * `//` (ou plus de 2 caractères `/`) suivi de texte jusqu’à la fin de la ligne. Les espaces après `/` ne sont pas obligatoires.
  * Peuvent s’étendre de `/*` à `*/` pour les commentaires multilignes. Les espaces ne sont pas non plus obligatoires.
  * Les commentaires de style C peuvent être imbriqués.

Par exemple :

```sql
/*
 * Compute the number of days between two dates.
 * /* Returns NULL if either argument is NULL */
 */
SELECT
    dateDiff('day', toDate('2024-01-01'), toDate('2024-12-31')) AS days_in_year, -- 365
    dateDiff('day', toDate('2020-01-01'), today()) AS days_since  #! since 2020
    ///////////////////////////////////////////////////////////////////
    # TODO: add hour/minute variants
```

<div id="keywords">
  ## Mots-clés
</div>

Dans ClickHouse, les mots-clés peuvent être soit *sensibles à la casse*, soit *insensibles à la casse*, selon le contexte.

Les mots-clés sont **insensibles à la casse** lorsqu&#39;ils correspondent à :

* la norme SQL. Par exemple, `SELECT`, `select` et `SeLeCt` sont tous valides.
* l&#39;implémentation de certains SGBD populaires (MySQL ou Postgres). Par exemple, `DateTime` est équivalent à `datetime`.

:::note
Vous pouvez vérifier si un nom de type de données est sensible à la casse dans la table [system.data&#95;type&#95;families](/fr/operations/system-tables/data_type_families).
:::

Contrairement au SQL standard, tous les autres mots-clés (y compris les noms de fonctions) sont **sensibles à la casse**.

En outre, les mots-clés ne sont pas réservés.
Ils ne sont traités comme tels que dans le contexte approprié.
Si vous utilisez des [identifiants](#identifiers) portant le même nom que des mots-clés, placez-les entre guillemets doubles ou entre backticks.

Par exemple, la requête suivante est valide si la table `table_name` contient une colonne nommée `"FROM"` :

```sql
SELECT "FROM" FROM table_name
```

<div id="identifiers">
  ## Identifiants
</div>

Les identifiants sont :

* Les noms de cluster, de base de données, de table, de partition et de colonne.
* Les [fonctions](#functions).
* Les [types de données](../sql-reference/data-types/index.md).
* Les [alias d’expression](#expression-aliases).

Les identifiants peuvent être cités ou non cités, même si la seconde forme est préférable.

Les identifiants non cités doivent correspondre à la regex `^[a-zA-Z_][0-9a-zA-Z_]*$` et ne peuvent pas être égaux à des [mots-clés](#keywords).
Voir le tableau ci-dessous pour des exemples d’identifiants valides et invalides :

| Identifiants valides                           | Identifiants invalides                 |
| ---------------------------------------------- | -------------------------------------- |
| `xyz`, `_internal`, `Id_with_underscores_123_` | `1x`, `tom@gmail.com`, `äußerst_schön` |

Si vous souhaitez utiliser des identifiants identiques à des mots-clés ou inclure d’autres symboles dans des identifiants, mettez-les entre guillemets doubles ou entre accents graves, par exemple : `"id"`, `` `id` ``.

:::note
Les mêmes règles d’échappement qui s’appliquent aux identifiants cités s’appliquent également aux littéraux de chaîne. Voir [String](#string) pour plus de détails.
:::

:::tip[Évitez d’utiliser des points dans les noms de colonnes]
Les noms de colonnes contenant des points, les colonnes partageant un même préfixe avec point et les colonnes de type `Array` peuvent tous être interprétés comme faisant partie d’une structure `Nested` aplatie lorsque `flatten_nested = 1` (valeur par défaut). Cela peut entraîner une validation inattendue de la longueur des tableaux lors des insertions, ainsi que des restrictions de renommage.

Évitez si possible d’utiliser des points dans les noms de colonnes.
Utilisez des traits de soulignement (`_`) ou un autre séparateur à la place des points dans les noms de colonnes, sauf si vous avez délibérément besoin de la sémantique `Nested`.
:::

<div id="literals">
  ## Littéraux
</div>

Dans ClickHouse, un littéral est une valeur représentée directement dans une query.
Autrement dit, c&#39;est une valeur fixe qui ne change pas pendant l&#39;exécution de la query.

Les littéraux peuvent être :

* [String](#string)
* [Numérique](#numeric)
* [Composé](#compound)
* [`NULL`](#null)
* [Heredocs](#heredoc) (littéraux de chaîne personnalisés)

Nous examinons chacun d&#39;eux plus en détail dans les sections ci-dessous.

<div id="string">
  ### String
</div>

Les littéraux de chaîne doivent être entourés de guillemets simples. Les guillemets doubles ne sont pas pris en charge.

L’échappement fonctionne de l’une des façons suivantes :

* en utilisant un guillemet simple, où le caractère guillemet simple `'` (et lui seul) peut être échappé sous la forme `''`, ou
* en utilisant une barre oblique inverse avec les séquences d’échappement prises en charge ci-dessous, répertoriées dans le tableau suivant.

:::note
La barre oblique inverse perd sa signification spéciale, c’est-à-dire qu’elle est interprétée littéralement si elle précède des caractères autres que ceux répertoriés ci-dessous.
:::

| Échappement pris en charge                 | Description                                                                                         |
| ------------------------------------------ | --------------------------------------------------------------------------------------------------- |
| `\xHH`                                     | Spécification d’un caractère sur 8 bits suivie d’un nombre quelconque de chiffres hexadécimaux (H). |
| `\N`                                       | réservé, ne fait rien (par ex. `SELECT 'a\Nb'` renvoie `ab`)                                        |
| `\a`                                       | alerte                                                                                              |
| `\b`                                       | retour arrière                                                                                      |
| `\e`                                       | caractère d’échappement                                                                             |
| `\f`                                       | saut de page                                                                                        |
| `\n`                                       | saut de ligne                                                                                       |
| `\r`                                       | retour chariot                                                                                      |
| `\t`                                       | tabulation horizontale                                                                              |
| `\v`                                       | tabulation verticale                                                                                |
| `\0`                                       | caractère nul                                                                                       |
| `\\`                                       | barre oblique inverse                                                                               |
| `\'` (ou `''`)                             | guillemet simple                                                                                    |
| `\"`                                       | guillemet double                                                                                    |
| `` ` ``                                    | accent grave                                                                                        |
| `\/`                                       | barre oblique                                                                                       |
| `\=`                                       | signe égal                                                                                          |
| Caractères de contrôle ASCII (c &lt;= 31). |                                                                                                     |

:::note
Dans les littéraux de chaîne, vous devez au minimum échapper `'` et `\` à l’aide des codes d’échappement `\'` (ou : `''`) et `\\`.
:::

<div id="numeric">
  ### Numérique
</div>

Les littéraux numériques sont analysés comme suit :

* Si le littéral est préfixé par un signe moins `-`, le token est ignoré et la négation est appliquée au résultat après l&#39;analyse.
* Le littéral numérique est d&#39;abord analysé comme un entier non signé de 64 bits, à l&#39;aide de la fonction [strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul).
  * Si la valeur est préfixée par `0b` ou `0x`/`0X`, le nombre est analysé respectivement comme binaire ou hexadécimal.
  * Si la valeur est négative et que sa valeur absolue est supérieure à 2<sup>63</sup>, une erreur est renvoyée.
* En cas d&#39;échec, la valeur est ensuite analysée comme un nombre à virgule flottante à l&#39;aide de la fonction [strtod](https://en.cppreference.com/w/cpp/string/byte/strtof).
* Sinon, une erreur est renvoyée.

Les valeurs littérales sont converties dans le plus petit type pouvant les contenir.
Par exemple :

* `1` est analysé comme `UInt8`
* `256` est analysé comme `UInt16`.

:::note Important
Les valeurs entières supérieures à 64 bits (`UInt128`, `Int128`, `UInt256`, `Int256`) doivent être converties vers un type plus grand pour être correctement analysées :

```sql
-170141183460469231731687303715884105728::Int128
340282366920938463463374607431768211455::UInt128
-57896044618658097711785492504343953926634992332820282019728792003956564819968::Int256
115792089237316195423570985008687907853269984665640564039457584007913129639935::UInt256
```

Cela contourne l&#39;algorithme ci-dessus et analyse l&#39;entier à l&#39;aide d&#39;une routine prenant en charge une précision arbitraire.

Sinon, le littéral sera interprété comme un nombre à virgule flottante et pourra donc subir une perte de précision due à la troncature.
:::

Pour plus d&#39;informations, voir [Types de données](../sql-reference/data-types/index.md).

Les traits de soulignement `_` à l&#39;intérieur des littéraux numériques sont ignorés et peuvent être utilisés pour améliorer la lisibilité.

Les littéraux numériques suivants sont pris en charge :

| Littéral numérique                                   | Exemples                                        |
| ---------------------------------------------------- | ----------------------------------------------- |
| **Entiers**                                          | `1`, `10_000_000`, `18446744073709551615`, `01` |
| **Décimaux**                                         | `0.1`                                           |
| **Notation exponentielle**                           | `1e100`, `-1e-100`                              |
| **Nombres à virgule flottante**                      | `123.456`, `inf`, `nan`                         |
| **Hexadécimal**                                      | `0xc0fe`                                        |
| **Chaîne hexadécimale compatible avec la norme SQL** | `x'c0fe'`                                       |
| **Binaire**                                          | `0b1101`                                        |
| **Chaîne binaire compatible avec la norme SQL**      | `b'1101'`                                       |

:::note
Les littéraux octaux ne sont pas pris en charge afin d&#39;éviter toute erreur d&#39;interprétation accidentelle.
:::

<div id="compound">
  ### Composés
</div>

Les tableaux se construisent avec `[]` : `[1, 2, 3]`. Les tuples se construisent avec `()` : `(1, 'Hello, world!', 2)`.
Techniquement, il ne s&#39;agit pas de littéraux, mais respectivement d&#39;expressions utilisant l&#39;opérateur de création de tableau et l&#39;opérateur de création de tuple.
Un tableau doit contenir au moins un élément, et un tuple au moins deux.

:::note
Il existe un cas distinct où des tuples apparaissent dans la clause `IN` d&#39;une requête `SELECT`.
Le résultat de la requête peut inclure des tuples, mais les tuples ne peuvent pas être enregistrés dans une base de données (sauf pour les tables utilisant le moteur [Memory](../engines/table-engines/special/memory.md)).
:::

<div id="null">
  ### NULL
</div>

`NULL` est utilisé pour indiquer qu&#39;une valeur est manquante.
Pour stocker `NULL` dans un champ de table, celui-ci doit être du type [Nullable](../sql-reference/data-types/nullable.md).

:::note
Les points suivants sont à noter concernant `NULL` :

* Selon le format de données (en entrée ou en sortie), `NULL` peut avoir une représentation différente. Pour plus d&#39;informations, consultez les [formats de données](/fr/interfaces/formats).
* Le traitement de `NULL` comporte certaines subtilités. Par exemple, si au moins un des arguments d&#39;une opération de comparaison est `NULL`, le résultat de cette opération est lui aussi `NULL`. Il en va de même pour la multiplication, l&#39;addition et les autres opérations. Nous vous recommandons de consulter la documentation de chaque opération.
* Dans les requêtes, vous pouvez tester `NULL` à l&#39;aide des opérateurs [`IS NULL`](/fr/sql-reference/functions/functions-for-nulls#isNull) et [`IS NOT NULL`](/fr/sql-reference/functions/functions-for-nulls#isNotNull), ainsi que des fonctions associées `isNull` et `isNotNull`.
  :::

<div id="heredoc">
  ### Heredoc
</div>

Un [heredoc](https://en.wikipedia.org/wiki/Here_document) est une façon de définir une chaîne de caractères (souvent sur plusieurs lignes) tout en conservant la mise en forme d’origine.
Un heredoc est défini comme une chaîne littérale personnalisée, placée entre deux symboles `$`.

Par exemple :

```sql
SELECT $heredoc$SHOW CREATE VIEW my_view$heredoc$;

┌─'SHOW CREATE VIEW my_view'─┐
│ SHOW CREATE VIEW my_view   │
└────────────────────────────┘
```

:::note

* Une valeur située entre deux heredocs est traitée &quot;telle quelle&quot;.
  :::

:::tip

* Vous pouvez utiliser un heredoc pour inclure des extraits de code SQL, HTML ou XML, entre autres.
  :::

<div id="defining-and-using-query-parameters">
  ## Définition et utilisation des paramètres de requête
</div>

Les paramètres de requête vous permettent d’écrire des requêtes génériques contenant des espaces réservés abstraits au lieu d’identifiants concrets.
Lorsqu’une requête avec paramètres de requête est exécutée,
tous les espaces réservés sont résolus et remplacés par les valeurs réelles des paramètres de requête.

Les paramètres de requête peuvent être définis de plusieurs façons :

* `SET param_<name>=<value>` — à l’aide d’une commande `SET` dans une requête.
* `--param_<name>='<value>'` — comme argument de `clickhouse-client` en ligne de commande.
* `param_<name>=<value>` — comme paramètre de chaîne de requête d’URL pour l’interface HTTP.

Un paramètre de requête peut être utilisé dans une requête avec la syntaxe `{<name>: <datatype>}`, où `<name>` est le nom du paramètre de requête et `<datatype>` le type de données vers lequel il est converti.

<details>
  <summary>Exemple avec la commande SET</summary>

  Par exemple, le SQL suivant définit des paramètres nommés `a`, `b`, `c` et `d` — chacun avec un type de données différent :

  ```sql
  SET param_a = 13;
  SET param_b = 'str';
  SET param_c = '2022-08-04 18:30:53';
  SET param_d = {'10': [11, 12], '13': [14, 15]};

  SELECT
     {a: UInt32},
     {b: String},
     {c: DateTime},
     {d: Map(String, Array(UInt8))};

  13    str    2022-08-04 18:30:53    {'10':[11,12],'13':[14,15]}
  ```
</details>

<details>
  <summary>Exemple avec clickhouse-client</summary>

  Si vous utilisez `clickhouse-client`, les paramètres sont spécifiés sous la forme `--param_name=value`. Par exemple, le paramètre suivant porte le nom `message` et est récupéré en tant que `String` :

  ```bash
  clickhouse-client --param_message='hello' --query="SELECT {message: String}"

  hello
  ```

  Si le paramètre de requête représente le nom d’une base de données, d’une table, d’une fonction ou d’un autre identifiant, utilisez `Identifier` comme type. Par exemple, la requête suivante renvoie des lignes d’une table nommée `uk_price_paid` :

  ```sql
  SET param_mytablename = "uk_price_paid";
  SELECT * FROM {mytablename:Identifier};
  ```
</details>

<details>
  <summary>Exemple avec l&#39;interface HTTP</summary>

  Les paramètres de requête peuvent être transmis comme paramètres de chaîne de requête d’URL avec le préfixe `param_`. Par exemple :

  ```bash
  curl -s "http://localhost:8123/?param_message=hello" --data-binary "SELECT {message: String}"

  hello
  ```
</details>

<details>
  <summary>Exemple avec la Web UI</summary>

  La Web UI intégrée (`play.html`) détecte automatiquement les espaces réservés de paramètres `{name:Type}` dans la requête et affiche des champs de saisie libellés pour chaque paramètre. Les valeurs des paramètres sont incluses dans la requête HTTP et également conservées dans l’URL de la page pour permettre l’ajout aux favoris et le partage.
</details>

:::note
Les paramètres de requête ne sont pas des substitutions de texte générales pouvant être utilisées à des emplacements arbitraires dans des requêtes SQL arbitraires.
Ils sont principalement conçus pour fonctionner dans les statements `SELECT` à la place d’identifiants ou de littéraux.
:::

<div id="functions">
  ## Fonctions
</div>

Les appels de fonction s’écrivent sous la forme d’un identifiant suivi d’une liste d’arguments (éventuellement vide) entre `()`.
Contrairement au SQL standard, les parenthèses sont obligatoires, même lorsque la liste d’arguments est vide.
Par exemple :

```sql
now()
```

Il existe également :

* [Fonctions régulières](/fr/sql-reference/functions/overview).
* [Fonctions d’agrégation](/fr/sql-reference/aggregate-functions).

Certaines fonctions d’agrégation peuvent comporter deux listes d’arguments entre parenthèses. Par exemple :

```sql
quantile (0.9)(x) 
```

Ces fonctions d’agrégation sont dites « paramétriques »,
et les arguments de la première liste sont appelés « paramètres ».

:::note
La syntaxe des fonctions d’agrégation sans paramètres est la même que celle des fonctions régulières.
:::

<div id="operators">
  ## Opérateurs
</div>

Les opérateurs sont convertis en fonctions correspondantes lors de l’analyse syntaxique de la requête, en tenant compte de leur priorité et de leur associativité.

Par exemple, l’expression

```text
1 + 2 * 3 + 4
```

est converti en

```text
plus(plus(1, multiply(2, 3)), 4)`
```

<div id="data-types-and-database-table-engines">
  ## Types de données et moteurs de table de la base de données
</div>

Les types de données et les moteurs de table dans la requête `CREATE` s’écrivent de la même manière que les identifiants ou les fonctions.
Autrement dit, ils peuvent comporter ou non une liste d’arguments entre parenthèses.

Pour en savoir plus, consultez les sections :

* [Types de données](/fr/sql-reference/data-types/index.md)
* [Moteurs de table](/fr/engines/table-engines/index.md)
* [CREATE](/fr/sql-reference/statements/create/index.md).

<div id="expressions">
  ## Expressions
</div>

Une expression peut être l&#39;un des éléments suivants :

* une fonction
* un identifiant
* un littéral
* l&#39;application d&#39;un opérateur
* une expression entre parenthèses
* une sous-requête
* un astérisque

Elle peut également contenir un [alias](#expression-aliases).

Une liste d&#39;expressions se compose d&#39;une ou plusieurs expressions séparées par des virgules.
Les fonctions et les opérateurs peuvent, à leur tour, prendre des expressions comme arguments.

Une expression constante est une expression dont le résultat est connu lors de l&#39;analyse de la requête, c&#39;est-à-dire avant l&#39;exécution.
Par exemple, les expressions sur des littéraux sont des expressions constantes.

<div id="expression-aliases">
  ## Alias d’expression
</div>

Un alias est un nom défini par l’utilisateur pour désigner une [expression](#expressions) dans une requête.

```sql
expr AS alias
```

Les éléments de la syntaxe ci-dessus sont expliqués ci-dessous.

| Partie de la syntaxe | Description                                                                                                                                                                      | Exemple                                                                 | Remarques                                                                                                                                                   |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `AS`                 | Mot-clé utilisé pour définir des alias. Vous pouvez définir l&#39;alias d&#39;un nom de table ou d&#39;un nom de colonne dans une clause `SELECT` sans utiliser le mot-clé `AS`. | `SELECT table_name_alias.column_name FROM table_name table_name_alias`. | Dans la fonction [CAST](/fr/sql-reference/functions/type-conversion-functions#CAST), le mot-clé `AS` a un autre sens. Consultez la description de la fonction. |
| `expr`               | Toute expression prise en charge par ClickHouse.                                                                                                                                 | `SELECT column_name * 2 AS double FROM some_table`                      |                                                                                                                                                             |
| `alias`              | Nom donné à `expr`. Les alias doivent respecter la syntaxe des [identifiants](#identifiers).                                                                                     | `SELECT "table t".column_name FROM table_name AS "table t"`.            |                                                                                                                                                             |

<div id="notes-on-usage">
  ### Remarques sur l’utilisation
</div>

* Les alias sont globaux à l’échelle d’une requête ou d’une sous-requête, et vous pouvez définir un alias pour n’importe quelle expression dans n’importe quelle partie d’une requête. Par exemple :

```sql
SELECT (1 AS n) + 2, n`.
```

* Les alias ne sont pas visibles dans les sous-requêtes ni d&#39;une sous-requête à l&#39;autre. Par exemple, lors de l&#39;exécution de la requête suivante, ClickHouse génère l&#39;exception `Unknown identifier: num` :

```sql
`SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a`
```

* Si un alias est défini pour les colonnes de résultat dans la clause `SELECT` d’une sous-requête, ces colonnes sont visibles dans la requête externe. Par exemple :

```sql
SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`.
```

* Soyez prudent avec les alias identiques aux noms de colonnes ou de tables. Prenons l’exemple suivant :

```sql
CREATE TABLE t
(
    a Int,
    b Int
)
ENGINE = TinyLog();

SELECT
    argMax(a, b),
    sum(b) AS b
FROM t;

Received exception from server (version 18.14.17):
Code: 184. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: Aggregate function sum(b) is found inside another aggregate function in query.
```

Dans l’exemple précédent, nous avons déclaré la table `t` avec la colonne `b`.
Ensuite, lors de la sélection des données, nous avons défini l’alias `sum(b) AS b`.
Comme les alias sont globaux,
ClickHouse a substitué le littéral `b` dans l’expression `argMax(a, b)` par l’expression `sum(b)`.
Cette substitution est à l’origine de l’exception.

:::note
Vous pouvez modifier ce comportement par défaut en définissant [prefer&#95;column&#95;name&#95;to&#95;alias](/fr/operations/settings/settings#prefer_column_name_to_alias) sur `1`.
:::

<div id="asterisk">
  ## Astérisque
</div>

Dans une requête `SELECT`, un astérisque peut être utilisé à la place de l’expression.
Pour plus d’informations, voir la section [SELECT](/fr/sql-reference/statements/select/index.md#asterisk).