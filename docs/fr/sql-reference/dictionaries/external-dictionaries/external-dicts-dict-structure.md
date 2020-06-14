---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: "Cl\xE9 et champs du dictionnaire"
---

# Clé et champs du dictionnaire {#dictionary-key-and-fields}

Le `<structure>` la clause décrit la clé du dictionnaire et les champs disponibles pour les requêtes.

Description XML:

``` xml
<dictionary>
    <structure>
        <id>
            <name>Id</name>
        </id>

        <attribute>
            <!-- Attribute parameters -->
        </attribute>

        ...

    </structure>
</dictionary>
```

Les attributs sont décrits dans les éléments:

-   `<id>` — [La colonne de la clé](external-dicts-dict-structure.md#ext_dict_structure-key).
-   `<attribute>` — [Colonne de données](external-dicts-dict-structure.md#ext_dict_structure-attributes). Il peut y avoir un certain nombre d'attributs.

Requête DDL:

``` sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

Les attributs sont décrits dans le corps de la requête:

-   `PRIMARY KEY` — [La colonne de la clé](external-dicts-dict-structure.md#ext_dict_structure-key)
-   `AttrName AttrType` — [Colonne de données](external-dicts-dict-structure.md#ext_dict_structure-attributes). Il peut y avoir un certain nombre d'attributs.

## Clé {#ext_dict_structure-key}

ClickHouse prend en charge les types de clés suivants:

-   Touche numérique. `UInt64`. Défini dans le `<id>` tag ou en utilisant `PRIMARY KEY` mot.
-   Clé Composite. Ensemble de valeurs de types différents. Défini dans la balise `<key>` ou `PRIMARY KEY` mot.

Une structure xml peut contenir `<id>` ou `<key>`. DDL-requête doit contenir unique `PRIMARY KEY`.

!!! warning "Avertissement"
    Vous ne devez pas décrire clé comme un attribut.

### Touche Numérique {#ext_dict-numeric-key}

Type: `UInt64`.

Exemple de Configuration:

``` xml
<id>
    <name>Id</name>
</id>
```

Champs de Configuration:

-   `name` – The name of the column with keys.

Pour DDL-requête:

``` sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

-   `PRIMARY KEY` – The name of the column with keys.

### Clé Composite {#composite-key}

La clé peut être un `tuple` de tous les types de champs. Le [disposition](external-dicts-dict-layout.md) dans ce cas, doit être `complex_key_hashed` ou `complex_key_cache`.

!!! tip "Conseil"
    Une clé composite peut être constitué d'un seul élément. Cela permet d'utiliser une chaîne comme clé, par exemple.

La structure de clé est définie dans l'élément `<key>`. Les principaux champs sont spécifiés dans le même format que le dictionnaire [attribut](external-dicts-dict-structure.md). Exemple:

``` xml
<structure>
    <key>
        <attribute>
            <name>field1</name>
            <type>String</type>
        </attribute>
        <attribute>
            <name>field2</name>
            <type>UInt32</type>
        </attribute>
        ...
    </key>
...
```

ou

``` sql
CREATE DICTIONARY (
    field1 String,
    field2 String
    ...
)
PRIMARY KEY field1, field2
...
```

Pour une requête à l' `dictGet*` fonction, un tuple est passé comme clé. Exemple: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

## Attribut {#ext_dict_structure-attributes}

Exemple de Configuration:

``` xml
<structure>
    ...
    <attribute>
        <name>Name</name>
        <type>ClickHouseDataType</type>
        <null_value></null_value>
        <expression>rand64()</expression>
        <hierarchical>true</hierarchical>
        <injective>true</injective>
        <is_object_id>true</is_object_id>
    </attribute>
</structure>
```

ou

``` sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

Champs de Configuration:

| Balise                                               | Description                                                                                                                                                                                                                                                                                                                                                                      | Requis |
|------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| `name`                                               | Nom de la colonne.                                                                                                                                                                                                                                                                                                                                                               | Oui    |
| `type`                                               | Type de données ClickHouse.<br/>ClickHouse tente de convertir la valeur du dictionnaire vers le type de données spécifié. Par exemple, pour MySQL, le champ peut être `TEXT`, `VARCHAR`, ou `BLOB` dans la table source MySQL, mais il peut être téléchargé comme `String` à ClickHouse.<br/>[Nullable](../../../sql-reference/data-types/nullable.md) n'est pas pris en charge. | Oui    |
| `null_value`                                         | Valeur par défaut pour un élément inexistant.<br/>Dans l'exemple, c'est une chaîne vide. Vous ne pouvez pas utiliser `NULL` dans ce domaine.                                                                                                                                                                                                                                     | Oui    |
| `expression`                                         | [Expression](../../syntax.md#syntax-expressions) que ClickHouse s'exécute sur la valeur.<br/>L'expression peut être un nom de colonne dans la base de données SQL distante. Ainsi, vous pouvez l'utiliser pour créer un alias pour la colonne à distance.<br/><br/>Valeur par défaut: aucune expression.                                                                         | Aucun  |
| <a name="hierarchical-dict-attr"></a> `hierarchical` | Si `true`, l'attribut contient la valeur d'un parent clé de la clé actuelle. Voir [Dictionnaires Hiérarchiques](external-dicts-dict-hierarchical.md).<br/><br/>Valeur par défaut: `false`.                                                                                                                                                                                       | Aucun  |
| `injective`                                          | Indicateur qui indique si le `id -> attribute` l'image est [injective](https://en.wikipedia.org/wiki/Injective_function).<br/>Si `true`, ClickHouse peut automatiquement placer après le `GROUP BY` clause les requêtes aux dictionnaires avec injection. Habituellement, il réduit considérablement le montant de ces demandes.<br/><br/>Valeur par défaut: `false`.            | Aucun  |
| `is_object_id`                                       | Indicateur qui indique si la requête est exécutée pour un document MongoDB par `ObjectID`.<br/><br/>Valeur par défaut: `false`.                                                                                                                                                                                                                                                  | Aucun  |

## Voir Aussi {#see-also}

-   [Fonctions pour travailler avec des dictionnaires externes](../../../sql-reference/functions/ext-dict-functions.md).

[Article Original](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_structure/) <!--hide-->
