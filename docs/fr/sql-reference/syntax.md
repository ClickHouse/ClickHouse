---
machine_translated: true
machine_translated_rev: f865c9653f9df092694258e0ccdd733c339112f5
toc_priority: 31
toc_title: Syntaxe
---

# Syntaxe {#syntax}

Il existe deux types d’analyseurs dans le système: L’analyseur SQL complet (un analyseur de descente récursif) et l’analyseur de format de données (un analyseur de flux rapide).
Dans tous les cas à l’exception de la `INSERT` requête, seul L’analyseur SQL complet est utilisé.
Le `INSERT` requête utilise les deux analyseurs:

``` sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

Le `INSERT INTO t VALUES` fragment est analysé par l’analyseur complet, et les données `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` est analysé par l’analyseur de flux rapide. Vous pouvez également activer l’analyseur complet pour les données à l’aide de la [input\_format\_values\_interpret\_expressions](../operations/settings/settings.md#settings-input_format_values_interpret_expressions) paramètre. Lorsque `input_format_values_interpret_expressions = 1`, ClickHouse essaie d’abord d’analyser les valeurs avec l’analyseur de flux rapide. S’il échoue, ClickHouse essaie d’utiliser l’analyseur complet pour les données, en le traitant comme un SQL [expression](#syntax-expressions).

Les données peuvent avoir n’importe quel format. Lorsqu’une requête est reçue, le serveur calcule pas plus que [max\_query\_size](../operations/settings/settings.md#settings-max_query_size) octets de la requête en RAM (par défaut, 1 Mo), et le reste est analysé en flux.
Cela signifie que le système n’a pas de problèmes avec de grandes `INSERT` requêtes, comme le fait MySQL.

Lors de l’utilisation de la `Values` format dans un `INSERT` de la requête, il peut sembler que les données sont analysées de même que les expressions dans un `SELECT` requête, mais ce n’est pas vrai. Le `Values` le format est beaucoup plus limitée.

Ensuite, nous allons couvrir l’analyseur complet. Pour plus d’informations sur les analyseurs de format, consultez [Format](../interfaces/formats.md) section.

## Espace {#spaces}

Il peut y avoir n’importe quel nombre de symboles d’espace entre les constructions syntaxiques (y compris le début et la fin d’une requête). Les symboles d’espace incluent l’espace, l’onglet, le saut de ligne, Le CR et le flux de formulaire.

## Commentaire {#comments}

Les commentaires de style SQL et C sont pris en charge.
Commentaires de style SQL: de `--` à la fin de la ligne. L’espace après `--` peut être omis.
Commentaires dans C-style: de `/*` de `*/`. Ces commentaires peuvent être multilignes. Les espaces ne sont pas requis ici non plus.

## Mot {#syntax-keywords}

Les mots clés sont insensibles à la casse lorsqu’ils correspondent à:

-   La norme SQL. Exemple, `SELECT`, `select` et `SeLeCt` sont toutes valides.
-   Implémentation dans certains SGBD populaires (MySQL ou Postgres). Exemple, `DateTime` est la même que `datetime`.

Si le nom du type de données est sensible à la casse peut être vérifié `system.data_type_families` table.

Contrairement à SQL standard, tous les autres mots clés (y compris les noms de fonctions) sont **sensible à la casse**.

Mots-clés ne sont pas réservés (ils sont simplement considérés comme des mots-clés dans le contexte correspondant). Si vous utilisez [identificateur](#syntax-identifiers) de même que les mots clés, les placer entre guillemets. Par exemple, la requête `SELECT "FROM" FROM table_name` est valide si la table `table_name` a colonne avec le nom de `"FROM"`.

## Identificateur {#syntax-identifiers}

Les identificateurs sont:

-   Noms de Cluster, de base de données, de table, de partition et de colonne.
-   Fonction.
-   Types de données.
-   [Expression des alias](#syntax-expression_aliases).

Les identificateurs peuvent être cités ou non cités. Il est recommandé d’utiliser des identificateurs sans guillemets.

Non identificateurs doivent correspondre à l’expression régulière `^[a-zA-Z_][0-9a-zA-Z_]*$` et ne peut pas être égale à [mot](#syntax-keywords). Exemple: `x, _1, X_y__Z123_.`

Si vous souhaitez utiliser les identifiants de la même manière que les mots-clés ou si vous souhaitez utiliser d’autres symboles dans les identifiants, citez-le en utilisant des guillemets doubles ou des backticks, par exemple, `"id"`, `` `id` ``.

## Littéral {#literals}

Il y a: Numérique, chaîne, composé et `NULL` littéral.

### Numérique {#numeric}

Un littéral numérique tente d’être analysé:

-   D’abord comme un nombre signé 64 bits, en utilisant le [strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul) fonction.
-   En cas d’échec, en tant que nombre non signé 64 bits, [strtoll](https://en.cppreference.com/w/cpp/string/byte/strtol) fonction.
-   En cas d’échec, en tant que nombre à virgule flottante [strtod](https://en.cppreference.com/w/cpp/string/byte/strtof) fonction.
-   Sinon, une erreur est renvoyée.

La valeur correspondante aura le plus petit type dans lequel la valeur correspond.
Par exemple, 1 est analysé comme `UInt8`, mais 256 est analysé comme `UInt16`. Pour plus d’informations, voir [Types de données](../sql-reference/data-types/index.md).

Exemple: `1`, `18446744073709551615`, `0xDEADBEEF`, `01`, `0.1`, `1e100`, `-1e-100`, `inf`, `nan`.

### Chaîne {#syntax-string-literal}

Seuls les littéraux de chaîne entre guillemets simples sont pris en charge. Le clos de caractères barre oblique inverse échappé. Les séquences d’échappement suivantes ont une valeur spéciale correspondante: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\a`, `\v`, `\xHH`. Dans tous les autres cas, des séquences d’échappement au format `\c`, où `c` est un caractère, sont convertis à `c`. Cela signifie que vous pouvez utiliser les séquences `\'`et`\\`. La valeur aurez l’ [Chaîne](../sql-reference/data-types/string.md) type.

L’ensemble minimum de caractères que vous devez échapper dans les littéraux de chaîne: `'` et `\`. Apostrophe peut être échappé avec l’apostrophe, les littéraux `'It\'s'` et `'It''s'` sont égaux.

### Composé {#compound}

Les Constructions sont prises en charge pour les tableaux: `[1, 2, 3]` et les tuples: `(1, 'Hello, world!', 2)`..
En fait, ce ne sont pas des littéraux, mais des expressions avec l’opérateur de création de tableau et l’opérateur de création de tuple, respectivement.
Un tableau doit être composé d’au moins un élément, et un tuple doit avoir au moins deux éléments.
Les Tuples ont un but spécial pour l’usage dans le `IN` clause de a `SELECT` requête. Les Tuples peuvent être obtenus à la suite d’une requête, mais ils ne peuvent pas être enregistrées dans une base de données (à l’exception de [Mémoire](../engines/table-engines/special/memory.md) table).

### NULL {#null-literal}

Indique que la valeur est manquante.

Afin de stocker `NULL` dans un champ de table, il doit être de la [Nullable](../sql-reference/data-types/nullable.md) type.

Selon le format de données (entrée ou sortie), `NULL` peut avoir une représentation différente. Pour plus d’informations, consultez la documentation de [formats de données](../interfaces/formats.md#formats).

Il y a beaucoup de nuances au traitement `NULL`. Par exemple, si au moins l’un des arguments d’une opération de comparaison est `NULL` le résultat de cette opération sera également `NULL`. Il en va de même pour la multiplication, l’addition et d’autres opérations. Pour plus d’informations, lisez la documentation pour chaque opération.

Dans les requêtes, vous pouvez vérifier `NULL` à l’aide de la [IS NULL](operators.md#operator-is-null) et [IS NOT NULL](operators.md) opérateurs et les fonctions connexes `isNull` et `isNotNull`.

## Fonction {#functions}

Les fonctions sont écrites comme un identifiant avec une liste d’arguments (éventuellement vide) entre parenthèses. Contrairement au SQL standard, les crochets sont requis, même pour une liste d’arguments vide. Exemple: `now()`.
Il existe des fonctions régulières et agrégées (voir la section “Aggregate functions”). Certaines fonctions d’agrégat peut contenir deux listes d’arguments entre parenthèses. Exemple: `quantile (0.9) (x)`. Ces fonctions d’agrégation sont appelés “parametric” fonctions, et les arguments dans la première liste sont appelés “parameters”. La syntaxe des fonctions d’agrégation sans paramètres est la même que pour les fonctions régulières.

## Opérateur {#operators}

Les opérateurs sont convertis en leurs fonctions correspondantes lors de l’analyse des requêtes, en tenant compte de leur priorité et de leur associativité.
Par exemple, l’expression `1 + 2 * 3 + 4` est transformé à `plus(plus(1, multiply(2, 3)), 4)`.

## Types De données Et Moteurs De Table De Base De données {#data_types-and-database-table-engines}

Types de données et moteurs de table dans `CREATE` les requêtes sont écrites de la même manière que les identifiants ou les fonctions. En d’autres termes, ils peuvent ou non contenir une liste d’arguments entre parenthèses. Pour plus d’informations, voir les sections “Data types,” “Table engines,” et “CREATE”.

## Expression Des Alias {#syntax-expression_aliases}

Un alias est un nom défini par l’utilisateur pour une expression dans une requête.

``` sql
expr AS alias
```

-   `AS` — The keyword for defining aliases. You can define the alias for a table name or a column name in a `SELECT` clause sans utiliser le `AS` mot.

        For example, `SELECT table_name_alias.column_name FROM table_name table_name_alias`.

        In the [CAST](sql_reference/functions/type_conversion_functions.md#type_conversion_function-cast) function, the `AS` keyword has another meaning. See the description of the function.

-   `expr` — Any expression supported by ClickHouse.

        For example, `SELECT column_name * 2 AS double FROM some_table`.

-   `alias` — Name for `expr`. Les alias doivent être conformes à la [identificateur](#syntax-identifiers) syntaxe.

        For example, `SELECT "table t".column_name FROM table_name AS "table t"`.

### Notes Sur l’Utilisation De La {#notes-on-usage}

Les alias sont globaux pour une requête ou d’une sous-requête et vous pouvez définir un alias dans n’importe quelle partie d’une requête de toute expression. Exemple, `SELECT (1 AS n) + 2, n`.

Les alias ne sont pas visibles dans les sous-requêtes et entre les sous-requêtes. Par exemple, lors de l’exécution de la requête `SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a` Clickhouse génère l’exception `Unknown identifier: num`.

Si un alias est défini pour les colonnes de `SELECT` la clause d’une sous-requête, ces colonnes sont visibles dans la requête externe. Exemple, `SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`.

Soyez prudent avec les Alias qui sont les mêmes que les noms de colonnes ou de tables. Considérons l’exemple suivant:

``` sql
CREATE TABLE t
(
    a Int,
    b Int
)
ENGINE = TinyLog()
```

``` sql
SELECT
    argMax(a, b),
    sum(b) AS b
FROM t
```

``` text
Received exception from server (version 18.14.17):
Code: 184. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: Aggregate function sum(b) is found inside another aggregate function in query.
```

Dans cet exemple, nous avons déclaré table `t` avec la colonne `b`. Ensuite, lors de la sélection des données, nous avons défini le `sum(b) AS b` alias. Comme les alias sont globaux, ClickHouse a substitué le littéral `b` dans l’expression `argMax(a, b)` avec l’expression `sum(b)`. Cette substitution a provoqué l’exception.

## Astérisque {#asterisk}

Dans un `SELECT` requête, un astérisque peut remplacer l’expression. Pour plus d’informations, consultez la section “SELECT”.

## Expression {#syntax-expressions}

Une expression est une fonction, un identifiant, un littéral, une application d’un opérateur, une expression entre parenthèses, une sous-requête ou un astérisque. Il peut également contenir un alias.
Une liste des expressions est une ou plusieurs expressions séparées par des virgules.
Les fonctions et les opérateurs, à leur tour, peuvent avoir des expressions comme arguments.

[Article Original](https://clickhouse.tech/docs/en/query_language/syntax/) <!--hide-->
