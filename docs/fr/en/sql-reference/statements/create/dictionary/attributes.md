---
description: 'Configuration de la clé et des attributs du dictionnaire'
sidebar_label: 'Attributs'
sidebar_position: 2
slug: /sql-reference/statements/create/dictionary/attributes
title: 'Attributs du dictionnaire'
doc_type: 'référence'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';

<CloudDetails />

La clause `structure` décrit la clé du dictionnaire ainsi que les champs disponibles pour les requêtes.

Description XML :

```xml
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

Les attributs sont décrits par les éléments suivants :

* `<id>` — Colonne clé
* `<attribute>` — Colonne de données : il peut y avoir plusieurs attributs.

Requête DDL :

```sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

Les attributs sont décrits dans le corps de la requête :

* `PRIMARY KEY` — Colonne clé
* `AttrName AttrType` — Colonne de données. Il peut y avoir plusieurs attributs.

<div id="key">
  ## Clé
</div>

ClickHouse prend en charge les types de clés suivants :

* Clé numérique. `UInt64`. Définie dans la balise `<id>` ou à l’aide du mot-clé `PRIMARY KEY`.
* Clé composite. Ensemble de valeurs de types différents. Définie dans la balise `<key>` ou à l’aide du mot-clé `PRIMARY KEY`.

Une structure XML peut contenir soit `<id>`, soit `<key>`. La requête DDL ne doit contenir qu’un seul `PRIMARY KEY`.

:::note
Vous ne devez pas décrire une clé comme un attribut.
:::

<div id="numeric-key">
  ### Clé numérique
</div>

Type : `UInt64`.

Exemple de configuration :

```xml
<id>
    <name>Id</name>
</id>
```

Champs de configuration :

* `name` – Le nom de la colonne contenant les clés.

Pour la requête DDL :

```sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

* `PRIMARY KEY` – Le nom de la colonne contenant les clés.

<div id="composite-key">
  ### Clé composite
</div>

La clé peut être un `tuple` constitué de champs de n&#39;importe quel type. Le [layout](./layouts/) doit alors être `complex_key_hashed` ou `complex_key_cache`.

:::tip
Une clé composite peut être constituée d&#39;un seul élément. Cela permet, par exemple, d&#39;utiliser une chaîne de caractères comme clé.
:::

La structure de la clé est définie dans l&#39;élément `<key>`. Les champs de clé sont spécifiés dans le même format que les [attributs](#attributes) du dictionnaire. Exemple :

```xml
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

or

```sql
CREATE DICTIONARY (
    field1 String,
    field2 UInt32
    ...
)
PRIMARY KEY field1, field2
...
```

Pour une query vers la fonction `dictGet*`, on passe un tuple comme clé. Exemple : `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

Lorsque la clé composite ne comporte qu’un seul attribut, la valeur de la clé peut être passée directement, sans être encapsulée dans `tuple`. Par exemple, `dictGetString('dict_name', 'attr_name', 'key')` et `dictGetString('dict_name', 'attr_name', tuple('key'))` sont tous deux valides.

<div id="attributes">
  ## Attributs
</div>

Exemple de configuration :

```xml
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

OR

```sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

Champs de configuration :

| Balise                                             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | Obligatoire |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `name`                                             | Nom de la colonne.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Oui         |
| `type`                                             | Type de données ClickHouse : [UInt8](../../../data-types/int-uint.md), [UInt16](../../../data-types/int-uint.md), [UInt32](../../../data-types/int-uint.md), [UInt64](../../../data-types/int-uint.md), [Int8](../../../data-types/int-uint.md), [Int16](../../../data-types/int-uint.md), [Int32](../../../data-types/int-uint.md), [Int64](../../../data-types/int-uint.md), [Float32](../../../data-types/float.md), [Float64](../../../data-types/float.md), [UUID](../../../data-types/uuid.md), [Decimal32](../../../data-types/decimal.md), [Decimal64](../../../data-types/decimal.md), [Decimal128](../../../data-types/decimal.md), [Decimal256](../../../data-types/decimal.md),[Date](../../../data-types/date.md), [Date32](../../../data-types/date32.md), [DateTime](../../../data-types/datetime.md), [DateTime64](../../../data-types/datetime64.md), [String](../../../data-types/string.md), [Array](../../../data-types/array.md).<br />ClickHouse essaie de convertir la valeur du dictionnaire dans le type de données spécifié. Par exemple, pour MySQL, le champ peut être de type `TEXT`, `VARCHAR` ou `BLOB` dans la table source MySQL, mais il peut être téléversé en tant que `String` dans ClickHouse.<br />[Nullable](../../../data-types/nullable.md) est actuellement pris en charge pour les dictionnaires [Flat](./layouts/flat), [Hashed](./layouts/hashed), [ComplexKeyHashed](./layouts/hashed#complex_key_hashed), [Direct](./layouts/direct), [ComplexKeyDirect](./layouts/direct#complex_key_direct), [RangeHashed](./layouts/range-hashed), Polygon, [Cache](./layouts/cache), [ComplexKeyCache](./layouts/cache), [SSDCache](./layouts/ssd-cache), [SSDComplexKeyCache](./layouts/ssd-cache#complex_key_ssd_cache). Dans les dictionnaires [IPTrie](./layouts/ip-trie), les types `Nullable` ne sont pas pris en charge. | Oui         |
| `null_value`                                       | Valeur par défaut pour un élément inexistant.<br />Dans l&#39;exemple, il s&#39;agit d&#39;une chaîne vide. La valeur [NULL](../../../syntax.md#null) peut être utilisée uniquement pour les types `Nullable` (voir la ligne précédente pour la description des types).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | Oui         |
| `expression`                                       | [Expression](../../../syntax.md#expressions) que ClickHouse exécute sur la valeur.<br />L&#39;expression peut être un nom de colonne dans la base de données SQL distante. Vous pouvez donc l&#39;utiliser pour créer un alias de la colonne distante.<br /><br />Valeur par défaut : aucune expression.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | Non         |
| <a name="hierarchical-dict-attr" /> `hierarchical` | Si `true`, l&#39;attribut contient la valeur d&#39;une clé parente pour la clé actuelle. Voir [Dictionnaires hiérarchiques](./layouts/hierarchical).<br /><br />Valeur par défaut : `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | Non         |
| `injective`                                        | Indicateur indiquant si l&#39;application `id -> attribute` est [injective](https://en.wikipedia.org/wiki/Injective_function).<br />Si `true`, ClickHouse peut automatiquement placer après la clause `GROUP BY` les requêtes vers les dictionnaires avec des attributs injectifs. En général, cela réduit considérablement le nombre de ces requêtes.<br /><br />Valeur par défaut : `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Non         |
| `is_object_id`                                     | Indicateur indiquant si la requête est exécutée sur un document MongoDB via `ObjectID`.<br /><br />Valeur par défaut : `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |             |