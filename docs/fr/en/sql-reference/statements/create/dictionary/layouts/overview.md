---
description: 'Types de layout de dictionnaire pour le stockage des dictionnaires en mémoire'
sidebar_label: 'Vue d’ensemble'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary/layouts
title: 'Layouts de dictionnaire'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="storing-dictionaries-in-memory">
  ## Types de layout des dictionnaires
</div>

Il existe différentes façons de stocker des dictionnaires en mémoire, chacune avec ses propres compromis en matière d&#39;utilisation du CPU et de la RAM.

| Layout                                                                                                     | Description                                                                                                                                                                   |
| ---------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [flat](./flat.md)                                                                                          | Stocke les données dans des tableaux plats indexés par clé. C&#39;est le layout le plus rapide, mais les clés doivent être de type `UInt64` et limitées par `max_array_size`. |
| [hashed](./hashed.md)                                                                                      | Stocke les données dans une table de hachage. Aucune limite sur la taille des clés, prend en charge n&#39;importe quel nombre d&#39;éléments.                                 |
| [sparse&#95;hashed](./hashed.md#sparse_hashed)                                                             | Comme `hashed`, mais sacrifie du CPU pour réduire l&#39;utilisation mémoire.                                                                                                  |
| [complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed)                                               | Comme `hashed`, pour les clés composées.                                                                                                                                      |
| [complex&#95;key&#95;sparse&#95;hashed](./hashed.md#complex_key_sparse_hashed)                             | Comme `sparse_hashed`, pour les clés composées.                                                                                                                               |
| [hashed&#95;array](./hashed-array.md)                                                                      | Attributs stockés dans des tableaux, avec une table de hachage qui associe les clés aux indices des tableaux. Efficace en mémoire pour de nombreux attributs.                 |
| [complex&#95;key&#95;hashed&#95;array](./hashed-array.md#complex_key_hashed_array)                         | Comme `hashed_array`, pour les clés composées.                                                                                                                                |
| [range&#95;hashed](./range-hashed.md)                                                                      | Table de hachage avec des plages ordonnées. Prend en charge les recherches par clé + plage de dates/heures.                                                                   |
| [complex&#95;key&#95;range&#95;hashed](./range-hashed.md#complex_key_range_hashed)                         | Comme `range_hashed`, pour les clés composées.                                                                                                                                |
| [cache](./cache.md)                                                                                        | Cache en mémoire de taille fixe. Seules les clés fréquemment consultées sont stockées.                                                                                        |
| [complex&#95;key&#95;cache](/fr/sql-reference/statements/create/dictionary/layouts/hashed#complex_key_hashed) | Comme `cache`, pour les clés composées.                                                                                                                                       |
| [ssd&#95;cache](./ssd-cache.md)                                                                            | Comme `cache`, mais stocke les données sur SSD avec un index en mémoire.                                                                                                      |
| [complex&#95;key&#95;ssd&#95;cache](./ssd-cache.md#complex_key_ssd_cache)                                  | Comme `ssd_cache`, pour les clés composées.                                                                                                                                   |
| [direct](./direct.md)                                                                                      | Aucun stockage en mémoire — interroge directement la source pour chaque requête.                                                                                              |
| [complex&#95;key&#95;direct](./direct.md#complex_key_direct)                                               | Comme `direct`, pour les clés composées.                                                                                                                                      |
| [ip&#95;trie](./ip-trie.md)                                                                                | Structure en trie pour des recherches rapides de préfixes IP (basées sur CIDR).                                                                                               |

:::tip Layouts recommandés
[flat](./flat.md), [hashed](./hashed.md) et [complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed) offrent les meilleures performances de requête.
Les layouts de cache ne sont pas recommandés en raison de performances potentiellement médiocres et de la difficulté à ajuster leurs paramètres — voir [cache](./cache.md) pour plus de détails.
:::

<div id="specify-dictionary-layout">
  ## Spécifier le layout du dictionnaire
</div>

<CloudDetails />

Vous pouvez configurer le layout d’un dictionnaire avec la clause `LAYOUT` (pour le DDL) ou le paramètre `layout` dans les définitions du fichier de configuration.

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY (...)
    ...
    LAYOUT(LAYOUT_TYPE(param value)) -- paramètres du layout
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <clickhouse>
        <dictionary>
            ...
            <layout>
                <layout_type>
                    <!-- paramètres du layout -->
                </layout_type>
            </layout>
            ...
        </dictionary>
    </clickhouse>
    ```
  </TabItem>
</Tabs>

<br />

Voir aussi [CREATE DICTIONARY](../overview.md) pour la syntaxe DDL complète.

Les dictionnaires dont le layout ne contient pas le mot `complex-key*` ont une clé de type [UInt64](/fr/sql-reference/data-types/int-uint.md) ; les dictionnaires `complex-key*` ont une clé composite (complexe, avec des types arbitraires).

**Exemple de clé numérique** (la colonne key&#95;column est de type [UInt64](/fr/sql-reference/data-types/int-uint.md)) :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        key_column UInt64,
        ...
    )
    PRIMARY KEY key_column
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <structure>
        <id>
            <name>key_column</name>
        </id>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<br />

**Exemple de clé composite** (la clé contient un élément de type [String](/fr/sql-reference/data-types/string.md)) :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        country_code String,
        ...
    )
    PRIMARY KEY country_code
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <structure>
        <key>
            <attribute>
                <name>country_code</name>
                <type>String</type>
            </attribute>
        </key>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<div id="improve-performance">
  ## Améliorer les performances des dictionnaires
</div>

Il existe plusieurs façons d&#39;améliorer les performances des dictionnaires :

* Appelez la fonction qui travaille avec le dictionnaire après `GROUP BY`.
* Marquez comme injectifs les attributs à extraire.
  Un attribut est dit injectif si des clés différentes correspondent à des valeurs d&#39;attribut différentes.
  Ainsi, lorsque `GROUP BY` utilise une fonction qui récupère une valeur d&#39;attribut à partir de la clé, cette fonction est automatiquement sortie de `GROUP BY`.

ClickHouse génère une exception en cas d&#39;erreur liée aux dictionnaires.
Voici quelques exemples d&#39;erreurs :

* Le dictionnaire auquel on tente d&#39;accéder n&#39;a pas pu être chargé.
* Erreur lors de la requête d&#39;un dictionnaire `cached`.

Vous pouvez consulter la liste des dictionnaires et leur statut dans la table [system.dictionaries](/fr/operations/system-tables/dictionaries.md).