---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 59
toc_title: Travailler avec Yandex.Dictionnaires Metrica
---

# Fonctions pour travailler avec Yandex.Dictionnaires Metrica {#functions-for-working-with-yandex-metrica-dictionaries}

Pour que les fonctions ci-dessous fonctionnent, la configuration du serveur doit spécifier les chemins et les adresses pour obtenir tous les Yandex.Dictionnaires Metrica. Les dictionnaires sont chargés au premier appel de l'une de ces fonctions. Si les listes de référence ne peuvent pas être chargées, une exception est levée.

Pour plus d'informations sur la création de listes de références, consultez la section “Dictionaries”.

## Plusieurs Geobases {#multiple-geobases}

ClickHouse soutient le travail avec plusieurs géobases alternatives (hiérarchies régionales) simultanément, afin de soutenir diverses perspectives sur les pays auxquels appartiennent certaines régions.

Le ‘clickhouse-server’ config spécifie le fichier avec l'échelon régional::`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

Outre ce fichier, il recherche également les fichiers à proximité qui ont le symbole _ et tout suffixe ajouté au nom (avant l'extension de fichier).
Par exemple, il trouvera également le fichier `/opt/geo/regions_hierarchy_ua.txt` si présente.

`ua` est appelée la clé du dictionnaire. Pour un dictionnaire sans suffixe, la clé est une chaîne vide.

Tous les dictionnaires sont rechargés dans l'exécution (une fois toutes les secondes, comme défini dans le paramètre de configuration builtin_dictionaries_reload_interval, ou une fois par heure par défaut). Cependant, la liste des dictionnaires disponibles est définie une fois, lorsque le serveur démarre.

All functions for working with regions have an optional argument at the end – the dictionary key. It is referred to as the geobase.
Exemple:

``` sql
regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt
```

### regionToCity (id \[, geobase\]) {#regiontocityid-geobase}

Accepts a UInt32 number – the region ID from the Yandex geobase. If this region is a city or part of a city, it returns the region ID for the appropriate city. Otherwise, returns 0.

### regionToArea (id \[, geobase\]) {#regiontoareaid-geobase}

Convertit une région en une zone (tapez 5 dans la géobase). Dans tous les autres cas, cette fonction est la même que ‘regionToCity’.

``` sql
SELECT DISTINCT regionToName(regionToArea(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

``` text
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

### regionToDistrict(id \[, geobase\]) {#regiontodistrictid-geobase}

Convertit une région en district fédéral (type 4 dans la géobase). Dans tous les autres cas, cette fonction est la même que ‘regionToCity’.

``` sql
SELECT DISTINCT regionToName(regionToDistrict(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

``` text
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

### regionToCountry (id \[, geobase\]) {#regiontocountryid-geobase}

Convertit une région en un pays. Dans tous les autres cas, cette fonction est la même que ‘regionToCity’.
Exemple: `regionToCountry(toUInt32(213)) = 225` convertit Moscou (213) en Russie (225).

### regionToContinent(id \[, géobase\]) {#regiontocontinentid-geobase}

Convertit une région en continent. Dans tous les autres cas, cette fonction est la même que ‘regionToCity’.
Exemple: `regionToContinent(toUInt32(213)) = 10001` convertit Moscou (213) en Eurasie (10001).

### regionToTopContinent (#regiontotopcontinent) {#regiontotopcontinent-regiontotopcontinent}

Trouve le continent le plus élevé dans la hiérarchie de la région.

**Syntaxe**

``` sql
regionToTopContinent(id[, geobase]);
```

**Paramètre**

-   `id` — Region ID from the Yandex geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
-   `geobase` — Dictionary key. See [Plusieurs Geobases](#multiple-geobases). [Chaîne](../../sql-reference/data-types/string.md). Facultatif.

**Valeur renvoyée**

-   Identifiant du continent de haut niveau (ce dernier lorsque vous grimpez dans la hiérarchie des régions).
-   0, si il n'y a aucun.

Type: `UInt32`.

### regionToPopulation (id \[, geobase\]) {#regiontopopulationid-geobase}

Obtient la population d'une région.
La population peut être enregistrée dans des fichiers avec la géobase. Voir la section “External dictionaries”.
Si la population n'est pas enregistrée pour la région, elle renvoie 0.
Dans la géobase Yandex, la population peut être enregistrée pour les régions enfants, mais pas pour les régions parentes.

### regionIn(lhs, rhs \[, géobase\]) {#regioninlhs-rhs-geobase}

Vérifie si un ‘lhs’ région appartient à une ‘rhs’ région. Renvoie un nombre UInt8 égal à 1 s'il appartient, Ou 0 s'il n'appartient pas.
The relationship is reflexive – any region also belongs to itself.

### regionHierarchy (id \[, geobase\]) {#regionhierarchyid-geobase}

Accepts a UInt32 number – the region ID from the Yandex geobase. Returns an array of region IDs consisting of the passed region and all parents along the chain.
Exemple: `regionHierarchy(toUInt32(213)) = [213,1,3,225,10001,10000]`.

### regionToName(id \[, lang\]) {#regiontonameid-lang}

Accepts a UInt32 number – the region ID from the Yandex geobase. A string with the name of the language can be passed as a second argument. Supported languages are: ru, en, ua, uk, by, kz, tr. If the second argument is omitted, the language ‘ru’ is used. If the language is not supported, an exception is thrown. Returns a string – the name of the region in the corresponding language. If the region with the specified ID doesn't exist, an empty string is returned.

`ua` et `uk` les deux signifient ukrainien.

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/ym_dict_functions/) <!--hide-->
