---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 58
toc_title: Travailler avec des dictionnaires externes
---

# Fonctions pour travailler avec des dictionnaires externes {#ext_dict_functions}

Pour plus d'informations sur la connexion et la configuration de dictionnaires externes, voir [Dictionnaires externes](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

## dictGet {#dictget}

Récupère une valeur d'un dictionnaire externe.

``` sql
dictGet('dict_name', 'attr_name', id_expr)
dictGetOrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**Paramètre**

-   `dict_name` — Name of the dictionary. [Chaîne littérale](../syntax.md#syntax-string-literal).
-   `attr_name` — Name of the column of the dictionary. [Chaîne littérale](../syntax.md#syntax-string-literal).
-   `id_expr` — Key value. [Expression](../syntax.md#syntax-expressions) de retour d'un [UInt64](../../sql-reference/data-types/int-uint.md) ou [Tuple](../../sql-reference/data-types/tuple.md)- tapez la valeur en fonction de la configuration du dictionnaire.
-   `default_value_expr` — Value returned if the dictionary doesn't contain a row with the `id_expr` clé. [Expression](../syntax.md#syntax-expressions) renvoyer la valeur dans le type de données configuré pour `attr_name` attribut.

**Valeur renvoyée**

-   Si ClickHouse analyse l'attribut avec succès dans le [l'attribut type de données](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes), les fonctions renvoient la valeur du dictionnaire de l'attribut qui correspond à `id_expr`.

-   Si il n'y a pas la clé, correspondant à `id_expr` dans le dictionnaire, puis:

        - `dictGet` returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.
        - `dictGetOrDefault` returns the value passed as the `default_value_expr` parameter.

ClickHouse lève une exception si elle ne peut pas analyser la valeur de l'attribut ou si la valeur ne correspond pas au type de données d'attribut.

**Exemple**

Créer un fichier texte `ext-dict-text.csv` contenant les éléments suivants:

``` text
1,1
2,2
```

La première colonne est `id` la deuxième colonne est `c1`.

Configurer le dictionnaire externe:

``` xml
<yandex>
    <dictionary>
        <name>ext-dict-test</name>
        <source>
            <file>
                <path>/path-to/ext-dict-test.csv</path>
                <format>CSV</format>
            </file>
        </source>
        <layout>
            <flat />
        </layout>
        <structure>
            <id>
                <name>id</name>
            </id>
            <attribute>
                <name>c1</name>
                <type>UInt32</type>
                <null_value></null_value>
            </attribute>
        </structure>
        <lifetime>0</lifetime>
    </dictionary>
</yandex>
```

Effectuer la requête:

``` sql
SELECT
    dictGetOrDefault('ext-dict-test', 'c1', number + 1, toUInt32(number * 10)) AS val,
    toTypeName(val) AS type
FROM system.numbers
LIMIT 3
```

``` text
┌─val─┬─type───┐
│   1 │ UInt32 │
│   2 │ UInt32 │
│  20 │ UInt32 │
└─────┴────────┘
```

**Voir Aussi**

-   [Dictionnaires Externes](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md)

## dictHas {#dicthas}

Vérifie si une clé est présente dans un dictionnaire.

``` sql
dictHas('dict_name', id_expr)
```

**Paramètre**

-   `dict_name` — Name of the dictionary. [Chaîne littérale](../syntax.md#syntax-string-literal).
-   `id_expr` — Key value. [Expression](../syntax.md#syntax-expressions) de retour d'un [UInt64](../../sql-reference/data-types/int-uint.md)-le type de la valeur.

**Valeur renvoyée**

-   0, si il n'y a pas de clé.
-   1, si il y a une clé.

Type: `UInt8`.

## dictGetHierarchy {#dictgethierarchy}

Crée un tableau contenant tous les parents d'une clé dans le [hiérarchique dictionnaire](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-hierarchical.md).

**Syntaxe**

``` sql
dictGetHierarchy('dict_name', key)
```

**Paramètre**

-   `dict_name` — Name of the dictionary. [Chaîne littérale](../syntax.md#syntax-string-literal).
-   `key` — Key value. [Expression](../syntax.md#syntax-expressions) de retour d'un [UInt64](../../sql-reference/data-types/int-uint.md)-le type de la valeur.

**Valeur renvoyée**

-   Les Parents pour la clé.

Type: [Tableau (UInt64)](../../sql-reference/data-types/array.md).

## dictisine {#dictisin}

Vérifie l'ancêtre d'une clé à travers toute la chaîne hiérarchique dans le dictionnaire.

``` sql
dictIsIn('dict_name', child_id_expr, ancestor_id_expr)
```

**Paramètre**

-   `dict_name` — Name of the dictionary. [Chaîne littérale](../syntax.md#syntax-string-literal).
-   `child_id_expr` — Key to be checked. [Expression](../syntax.md#syntax-expressions) de retour d'un [UInt64](../../sql-reference/data-types/int-uint.md)-le type de la valeur.
-   `ancestor_id_expr` — Alleged ancestor of the `child_id_expr` clé. [Expression](../syntax.md#syntax-expressions) de retour d'un [UInt64](../../sql-reference/data-types/int-uint.md)-le type de la valeur.

**Valeur renvoyée**

-   0, si `child_id_expr` n'est pas un enfant de `ancestor_id_expr`.
-   1, si `child_id_expr` est un enfant de `ancestor_id_expr` ou si `child_id_expr` est un `ancestor_id_expr`.

Type: `UInt8`.

## D'Autres Fonctions {#ext_dict_functions-other}

ClickHouse prend en charge des fonctions spécialisées qui convertissent les valeurs d'attribut de dictionnaire en un type de données spécifique, quelle que soit la configuration du dictionnaire.

Fonction:

-   `dictGetInt8`, `dictGetInt16`, `dictGetInt32`, `dictGetInt64`
-   `dictGetUInt8`, `dictGetUInt16`, `dictGetUInt32`, `dictGetUInt64`
-   `dictGetFloat32`, `dictGetFloat64`
-   `dictGetDate`
-   `dictGetDateTime`
-   `dictGetUUID`
-   `dictGetString`

Toutes ces fonctions ont le `OrDefault` modification. Exemple, `dictGetDateOrDefault`.

Syntaxe:

``` sql
dictGet[Type]('dict_name', 'attr_name', id_expr)
dictGet[Type]OrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**Paramètre**

-   `dict_name` — Name of the dictionary. [Chaîne littérale](../syntax.md#syntax-string-literal).
-   `attr_name` — Name of the column of the dictionary. [Chaîne littérale](../syntax.md#syntax-string-literal).
-   `id_expr` — Key value. [Expression](../syntax.md#syntax-expressions) de retour d'un [UInt64](../../sql-reference/data-types/int-uint.md)-le type de la valeur.
-   `default_value_expr` — Value which is returned if the dictionary doesn't contain a row with the `id_expr` clé. [Expression](../syntax.md#syntax-expressions) renvoyer une valeur dans le type de données configuré pour `attr_name` attribut.

**Valeur renvoyée**

-   Si ClickHouse analyse l'attribut avec succès dans le [l'attribut type de données](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes), les fonctions renvoient la valeur du dictionnaire de l'attribut qui correspond à `id_expr`.

-   Si il n'est pas demandé `id_expr` dans le dictionnaire,:

        - `dictGet[Type]` returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.
        - `dictGet[Type]OrDefault` returns the value passed as the `default_value_expr` parameter.

ClickHouse lève une exception si elle ne peut pas analyser la valeur de l'attribut ou si la valeur ne correspond pas au type de données d'attribut.

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/ext_dict_functions/) <!--hide-->
