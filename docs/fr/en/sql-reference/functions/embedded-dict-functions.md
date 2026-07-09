---
description: 'Documentation des fonctions de manipulation des dictionnaires intégrés'
sidebar_label: 'Dictionnaire intégré'
slug: /sql-reference/functions/ym-dict-functions
title: 'Fonctions de manipulation des dictionnaires intégrés'
doc_type: 'reference'
---

:::note
Pour que les fonctions ci-dessous puissent fonctionner, la configuration du serveur doit spécifier les chemins et les adresses permettant d’accéder à tous les dictionnaires intégrés. Les dictionnaires sont chargés lors du premier appel à l’une de ces fonctions. Si les listes de référence ne peuvent pas être chargées, une exception est levée.

Par conséquent, les exemples présentés dans cette section lèveront une exception dans [ClickHouse Fiddle](https://fiddle.clickhouse.com/) ainsi que dans les déploiements quick release et de production par défaut, sauf s’ils sont configurés au préalable.
:::

Pour plus d’informations sur la création de listes de référence, consultez la section [&quot;Dictionnaires&quot;](../statements/create/dictionary/embedded).

<div id="multiple-geobases">
  ## Géobases multiples
</div>

ClickHouse prend en charge l’utilisation simultanée de plusieurs géobases alternatives (hiérarchies régionales), afin de refléter différents points de vue sur l’appartenance de certaines régions à certains pays.

La configuration de &#39;clickhouse-server&#39; spécifie le fichier contenant la hiérarchie régionale :

`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

En plus de ce fichier, il recherche également les fichiers situés à proximité dont le nom contient le symbole `_` suivi de n’importe quel suffixe (avant l’extension du fichier).
Par exemple, il trouvera aussi le fichier `/opt/geo/regions_hierarchy_ua.txt`, s’il est présent. Ici, `ua` est appelé la clé du dictionnaire. Pour un dictionnaire sans suffixe, la clé est une chaîne vide.

Tous les dictionnaires sont rechargés pendant l’exécution (à intervalles réguliers de quelques secondes, comme défini par le paramètre de configuration [`builtin_dictionaries_reload_interval`](/fr/operations/server-configuration-parameters/settings#builtin_dictionaries_reload_interval), ou une fois par heure par défaut). Cependant, la liste des dictionnaires disponibles n’est définie qu’une seule fois, au démarrage du serveur.

Toutes les fonctions permettant de travailler avec les régions acceptent un argument facultatif à la fin : la clé du dictionnaire. C’est ce qu’on appelle la géobase.

Exemple :

```sql
regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt
```

### regionToName

Accepte un ID de région et une geobase, et renvoie une chaîne de caractères contenant le nom de la région dans la langue correspondante. Si la région associée à l’ID spécifié n’existe pas, une chaîne vide est renvoyée.

**Syntaxe**

```sql
regionToName(id\[, lang\])
```

**Paramètres**

* `id` — ID de région issu de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — Clé du dictionnaire. Voir [Multiple Geobases](#multiple-geobases). [String](../data-types/string). Facultatif.

**Valeur renvoyée**

* Nom de la région dans la langue correspondante spécifiée par `geobase`. [String](../data-types/string).
* Sinon, une chaîne vide.

**Exemple**

```sql title="Query"
SELECT regionToName(number::UInt32,'en') FROM numbers(0,5);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┐
│                                            │
│ World                                      │
│ USA                                        │
│ Colorado                                   │
│ Boulder County                             │
└────────────────────────────────────────────┘
```

### regionToCity

Accepte un ID de région issu de la geobase. Si cette région est une ville ou fait partie d’une ville, renvoie l’ID de région de la ville correspondante. Sinon, renvoie 0.

**Syntaxe**

```sql
regionToCity(id [, geobase])
```

**Paramètres**

* `id` — ID de région issu de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — Clé de dictionnaire. Voir [Bases géographiques multiples](#multiple-geobases). [String](../data-types/string). Facultatif.

**Valeur renvoyée**

* ID de région de la ville correspondante, si elle existe. [UInt32](../data-types/int-uint).
* 0, s&#39;il n&#39;en existe pas.

**Exemple**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToCity(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```response title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToCity(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                          │
│ World                                      │  0 │                                                          │
│ USA                                        │  0 │                                                          │
│ Colorado                                   │  0 │                                                          │
│ Boulder County                             │  0 │                                                          │
│ Boulder                                    │  5 │ Boulder                                                  │
│ China                                      │  0 │                                                          │
│ Sichuan                                    │  0 │                                                          │
│ Chengdu                                    │  8 │ Chengdu                                                  │
│ America                                    │  0 │                                                          │
│ North America                              │  0 │                                                          │
│ Eurasia                                    │  0 │                                                          │
│ Asia                                       │  0 │                                                          │
└────────────────────────────────────────────┴────┴──────────────────────────────────────────────────────────┘
```

### regionToArea

Convertit une région en zone (type 5 dans la geobase). Par ailleurs, cette fonction est identique à [&#39;regionToCity&#39;](#regiontocity).

**Syntaxe**

```sql
regionToArea(id [, geobase])
```

**Paramètres**

* `id` — ID de région issu de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — Clé de dictionnaire. Voir [Multiple Geobases](#multiple-geobases). [String](../data-types/string). Facultatif.

**Valeur renvoyée**

* ID de région de la zone correspondante, s&#39;il existe. [UInt32](../data-types/int-uint).
* 0, sinon.

**Exemple**

```sql title="Query"
SELECT DISTINCT regionToName(regionToArea(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

```text title="Response"
┌─regionToName(regionToArea(toUInt32(number), \'ua\'))─┐
│                                                      │
│ Moscow and Moscow region                             │
│ St. Petersburg and Leningrad region                  │
│ Belgorod region                                      │
│ Ivanovsk region                                      │
│ Kaluga region                                        │
│ Kostroma region                                      │
│ Kursk region                                         │
│ Lipetsk region                                       │
│ Orlov region                                         │
│ Ryazan region                                        │
│ Smolensk region                                      │
│ Tambov region                                        │
│ Tver region                                          │
│ Tula region                                          │
└──────────────────────────────────────────────────────┘
```

### regionToDistrict

Convertit une région en district fédéral (type 4 dans la geobase). Pour tout le reste, cette fonction est identique à &#39;regionToCity&#39;.

**Syntaxe**

```sql
regionToDistrict(id [, geobase])
```

**Paramètres**

* `id` — ID de région de la géobase. [UInt32](../data-types/int-uint).
* `geobase` — Clé du dictionnaire. Voir [Géobases multiples](#multiple-geobases). [String](../data-types/string). Facultatif.

**Valeur renvoyée**

* ID de région de la ville correspondante, si elle existe. [UInt32](../data-types/int-uint).
* 0, s’il n’en existe aucune.

**Exemple**

```sql title="Query"
SELECT DISTINCT regionToName(regionToDistrict(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

```text title="Response"
┌─regionToName(regionToDistrict(toUInt32(number), \'ua\'))─┐
│                                                          │
│ Central federal district                                 │
│ Northwest federal district                               │
│ South federal district                                   │
│ North Caucases federal district                          │
│ Privolga federal district                                │
│ Ural federal district                                    │
│ Siberian federal district                                │
│ Far East federal district                                │
│ Scotland                                                 │
│ Faroe Islands                                            │
│ Flemish region                                           │
│ Brussels capital region                                  │
│ Wallonia                                                 │
│ Federation of Bosnia and Herzegovina                     │
└──────────────────────────────────────────────────────────┘
```

### regionToCountry

Convertit une région en pays (type 3 dans la geobase). Pour le reste, cette fonction est identique à &#39;regionToCity&#39;.

**Syntaxe**

```sql
regionToCountry(id [, geobase])
```

**Paramètres**

* `id` — ID de région issu de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — Clé du dictionnaire. Voir [Multiple Geobases](#multiple-geobases). [String](../data-types/string). Facultatif.

**Valeur retournée**

* ID de région du pays correspondant, s&#39;il existe. [UInt32](../data-types/int-uint).
* 0, sinon.

**Exemple**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToCountry(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToCountry(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                             │
│ World                                      │  0 │                                                             │
│ USA                                        │  2 │ USA                                                         │
│ Colorado                                   │  2 │ USA                                                         │
│ Boulder County                             │  2 │ USA                                                         │
│ Boulder                                    │  2 │ USA                                                         │
│ China                                      │  6 │ China                                                       │
│ Sichuan                                    │  6 │ China                                                       │
│ Chengdu                                    │  6 │ China                                                       │
│ America                                    │  0 │                                                             │
│ North America                              │  0 │                                                             │
│ Eurasia                                    │  0 │                                                             │
│ Asia                                       │  0 │                                                             │
└────────────────────────────────────────────┴────┴─────────────────────────────────────────────────────────────┘
```

### regionToContinent

Convertit une région en continent (type 1 dans la geobase). À tous autres égards, cette fonction est identique à &#39;regionToCity&#39;.

**Syntaxe**

```sql
regionToContinent(id [, geobase])
```

**Paramètres**

* `id` — ID de région de la base géographique. [UInt32](../data-types/int-uint).
* `geobase` — Clé de dictionnaire. Voir [Bases géographiques multiples](#multiple-geobases). [String](../data-types/string). Facultatif.

**Valeur renvoyée**

* ID de région du continent correspondant, s&#39;il en existe un. [UInt32](../data-types/int-uint).
* 0 dans le cas contraire.

**Exemple**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToContinent(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                               │
│ World                                      │  0 │                                                               │
│ USA                                        │ 10 │ North America                                                 │
│ Colorado                                   │ 10 │ North America                                                 │
│ Boulder County                             │ 10 │ North America                                                 │
│ Boulder                                    │ 10 │ North America                                                 │
│ China                                      │ 12 │ Asia                                                          │
│ Sichuan                                    │ 12 │ Asia                                                          │
│ Chengdu                                    │ 12 │ Asia                                                          │
│ America                                    │  9 │ America                                                       │
│ North America                              │ 10 │ North America                                                 │
│ Eurasia                                    │ 11 │ Eurasia                                                       │
│ Asia                                       │ 12 │ Asia                                                          │
└────────────────────────────────────────────┴────┴───────────────────────────────────────────────────────────────┘
```

### regionToTopContinent

Renvoie le continent le plus élevé dans la hiérarchie pour la région.

**Syntaxe**

```sql
regionToTopContinent(id[, geobase])
```

**Paramètres**

* `id` — Identifiant de région de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — Clé du dictionnaire. Voir [Multiple Geobases](#multiple-geobases). [String](../data-types/string). Facultatif.

**Valeur renvoyée**

* Identifiant du continent situé au niveau le plus élevé de la hiérarchie des régions. [UInt32](../data-types/int-uint).
* 0, s&#39;il n&#39;y en a pas.

**Exemple**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToTopContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToTopContinent(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                                  │
│ World                                      │  0 │                                                                  │
│ USA                                        │  9 │ America                                                          │
│ Colorado                                   │  9 │ America                                                          │
│ Boulder County                             │  9 │ America                                                          │
│ Boulder                                    │  9 │ America                                                          │
│ China                                      │ 11 │ Eurasia                                                          │
│ Sichuan                                    │ 11 │ Eurasia                                                          │
│ Chengdu                                    │ 11 │ Eurasia                                                          │
│ America                                    │  9 │ America                                                          │
│ North America                              │  9 │ America                                                          │
│ Eurasia                                    │ 11 │ Eurasia                                                          │
│ Asia                                       │ 11 │ Eurasia                                                          │
└────────────────────────────────────────────┴────┴──────────────────────────────────────────────────────────────────┘
```

### regionToPopulation

Renvoie la population d’une région. La population peut être consignée dans des fichiers avec la geobase. Voir la section [&quot;Dictionaries&quot;](../statements/create/dictionary/embedded). Si la population n’est pas renseignée pour la région, la fonction renvoie 0. Dans la geobase, la population peut être renseignée pour des régions de niveau inférieur, mais pas pour des régions de niveau supérieur.

**Syntaxe**

```sql
regionToPopulation(id[, geobase])
```

**Paramètres**

* `id` — ID de région issu de la géobase. [UInt32](../data-types/int-uint).
* `geobase` — Clé du dictionnaire. Voir [Multiple Geobases](#multiple-geobases). [String](../data-types/string). Facultatif.

**Valeur renvoyée**

* Population de la région. [UInt32](../data-types/int-uint).
* 0, si aucune n&#39;existe.

**Exemple**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToPopulation(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─population─┐
│                                            │          0 │
│ World                                      │ 4294967295 │
│ USA                                        │  330000000 │
│ Colorado                                   │    5700000 │
│ Boulder County                             │     330000 │
│ Boulder                                    │     100000 │
│ China                                      │ 1500000000 │
│ Sichuan                                    │   83000000 │
│ Chengdu                                    │   20000000 │
│ America                                    │ 1000000000 │
│ North America                              │  600000000 │
│ Eurasia                                    │ 4294967295 │
│ Asia                                       │ 4294967295 │
└────────────────────────────────────────────┴────────────┘
```

### regionIn

Vérifie si une région `lhs` fait partie d&#39;une région `rhs`. Renvoie un nombre UInt8 égal à 1 si c&#39;est le cas, ou à 0 dans le cas contraire.

**Syntaxe**

```sql
regionIn(lhs, rhs\[, geobase\])
```

**Paramètres**

* `lhs` — ID de région `lhs` de la geobase. [UInt32](../data-types/int-uint).
* `rhs` — ID de région `rhs` de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — Clé du dictionnaire. Voir [Plusieurs géobases](#multiple-geobases). [String](../data-types/string). Facultatif.

**Valeur renvoyée**

* 1, si oui. [UInt8](../data-types/int-uint).
* 0, sinon.

**Détails de l’implémentation**

La relation est réflexive – une région appartient également à elle-même.

**Exemple**

```sql title="Query"
SELECT regionToName(n1.number::UInt32, 'en') || (regionIn(n1.number::UInt32, n2.number::UInt32) ? ' is in ' : ' is not in ') || regionToName(n2.number::UInt32, 'en') FROM numbers(1,2) AS n1 CROSS JOIN numbers(1,5) AS n2;
```

```text title="Response"
World is in World
World is not in USA
World is not in Colorado
World is not in Boulder County
World is not in Boulder
USA is in World
USA is in USA
USA is not in Colorado
USA is not in Boulder County
USA is not in Boulder    
```

### regionHierarchy

Accepte un nombre UInt32 — l’ID de région de la geobase. Renvoie un tableau d’ID de région contenant la région fournie ainsi que tous ses parents dans la hiérarchie.

**Syntaxe**

```sql
regionHierarchy(id\[, geobase\])
```

**Paramètres**

* `id` — ID de région de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — Clé du dictionnaire. Voir [Multiple Geobases](#multiple-geobases). [String](../data-types/string). Facultatif.

**Valeur renvoyée**

* Tableau des ID de région comprenant la région transmise et tous ses parents dans la chaîne hiérarchique. [Array](../data-types/array)([UInt32](../data-types/int-uint)).

**Exemple**

```sql title="Query"
SELECT regionHierarchy(number::UInt32) AS arr, arrayMap(id -> regionToName(id, 'en'), arr) FROM numbers(5);
```

```text title="Response"
┌─arr────────────┬─arrayMap(lambda(tuple(id), regionToName(id, 'en')), regionHierarchy(CAST(number, 'UInt32')))─┐
│ []             │ []                                                                                           │
│ [1]            │ ['World']                                                                                    │
│ [2,10,9,1]     │ ['USA','North America','America','World']                                                    │
│ [3,2,10,9,1]   │ ['Colorado','USA','North America','America','World']                                         │
│ [4,3,2,10,9,1] │ ['Boulder County','Colorado','USA','North America','America','World']                        │
└────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────┘
```

{/* 
  Le contenu interne des balises ci-dessous est remplacé, lors du build du framework de documentation, par 
  de la documentation générée à partir de system.functions. Veuillez ne pas modifier ni supprimer ces balises.
  Voir : https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }