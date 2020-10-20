---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "Stockage des dictionnaires en m\xE9moire"
---

# Stockage des dictionnaires en mémoire {#dicts-external-dicts-dict-layout}

Il existe une variété de façons de stocker les dictionnaires en mémoire.

Nous vous recommandons [plat](#flat), [haché](#dicts-external_dicts_dict_layout-hashed) et [complex_key_hashed](#complex-key-hashed). qui fournissent la vitesse de traitement optimale.

La mise en cache n'est pas recommandée en raison de performances potentiellement médiocres et de difficultés à sélectionner les paramètres optimaux. En savoir plus dans la section “[cache](#cache)”.

Il existe plusieurs façons d'améliorer les performances du dictionnaire:

-   Appelez la fonction pour travailler avec le dictionnaire après `GROUP BY`.
-   Marquer les attributs à extraire comme injectifs. Un attribut est appelé injectif si différentes valeurs d'attribut correspondent à différentes clés. Alors, quand `GROUP BY` utilise une fonction qui récupère une valeur d'attribut par la clé, cette fonction est automatiquement retirée de `GROUP BY`.

ClickHouse génère une exception pour les erreurs avec les dictionnaires. Des exemples d'erreurs:

-   Le dictionnaire accessible n'a pas pu être chargé.
-   Erreur de la requête d'une `cached` dictionnaire.

Vous pouvez afficher la liste des dictionnaires externes et leurs statuts dans le `system.dictionaries` table.

La configuration ressemble à ceci:

``` xml
<yandex>
    <dictionary>
        ...
        <layout>
            <layout_type>
                <!-- layout settings -->
            </layout_type>
        </layout>
        ...
    </dictionary>
</yandex>
```

Correspondant [DDL-requête](../../statements/create.md#create-dictionary-query):

``` sql
CREATE DICTIONARY (...)
...
LAYOUT(LAYOUT_TYPE(param value)) -- layout settings
...
```

## Façons de stocker des dictionnaires en mémoire {#ways-to-store-dictionaries-in-memory}

-   [plat](#flat)
-   [haché](#dicts-external_dicts_dict_layout-hashed)
-   [sparse_hashed](#dicts-external_dicts_dict_layout-sparse_hashed)
-   [cache](#cache)
-   [direct](#direct)
-   [range_hashed](#range-hashed)
-   [complex_key_hashed](#complex-key-hashed)
-   [complex_key_cache](#complex-key-cache)
-   [complex_key_direct](#complex-key-direct)
-   [ip_trie](#ip-trie)

### plat {#flat}

Le dictionnaire est complètement stocké en mémoire sous la forme de tableaux plats. Combien de mémoire le dictionnaire utilise-t-il? Le montant est proportionnel à la taille de la plus grande clé (dans l'espace).

La clé du dictionnaire a le `UInt64` type et la valeur est limitée à 500 000. Si une clé plus grande est découverte lors de la création du dictionnaire, ClickHouse lève une exception et ne crée pas le dictionnaire.

Tous les types de sources sont pris en charge. Lors de la mise à jour, les données (à partir d'un fichier ou d'une table) sont lues dans leur intégralité.

Cette méthode fournit les meilleures performances parmi toutes les méthodes disponibles de stockage du dictionnaire.

Exemple de Configuration:

``` xml
<layout>
  <flat />
</layout>
```

ou

``` sql
LAYOUT(FLAT())
```

### haché {#dicts-external_dicts_dict_layout-hashed}

Le dictionnaire est entièrement stockée en mémoire sous la forme d'une table de hachage. Le dictionnaire peut contenir n'importe quel nombre d'éléments avec tous les identificateurs Dans la pratique, le nombre de clés peut atteindre des dizaines de millions d'articles.

Tous les types de sources sont pris en charge. Lors de la mise à jour, les données (à partir d'un fichier ou d'une table) sont lues dans leur intégralité.

Exemple de Configuration:

``` xml
<layout>
  <hashed />
</layout>
```

ou

``` sql
LAYOUT(HASHED())
```

### sparse_hashed {#dicts-external_dicts_dict_layout-sparse_hashed}

Semblable à `hashed`, mais utilise moins de mémoire en faveur de plus D'utilisation du processeur.

Exemple de Configuration:

``` xml
<layout>
  <sparse_hashed />
</layout>
```

``` sql
LAYOUT(SPARSE_HASHED())
```

### complex_key_hashed {#complex-key-hashed}

Ce type de stockage est pour une utilisation avec composite [touches](external-dicts-dict-structure.md). Semblable à `hashed`.

Exemple de Configuration:

``` xml
<layout>
  <complex_key_hashed />
</layout>
```

``` sql
LAYOUT(COMPLEX_KEY_HASHED())
```

### range_hashed {#range-hashed}

Le dictionnaire est stocké en mémoire sous la forme d'une table de hachage avec un tableau ordonné de gammes et leurs valeurs correspondantes.

Cette méthode de stockage fonctionne de la même manière que hachée et permet d'utiliser des plages de date / heure (Type numérique arbitraire) en plus de la clé.

Exemple: Le tableau contient des réductions pour chaque annonceur dans le format:

``` text
+---------|-------------|-------------|------+
| advertiser id | discount start date | discount end date | amount |
+===============+=====================+===================+========+
| 123           | 2015-01-01          | 2015-01-15        | 0.15   |
+---------|-------------|-------------|------+
| 123           | 2015-01-16          | 2015-01-31        | 0.25   |
+---------|-------------|-------------|------+
| 456           | 2015-01-01          | 2015-01-15        | 0.05   |
+---------|-------------|-------------|------+
```

Pour utiliser un échantillon pour les plages de dates, définissez `range_min` et `range_max` éléments dans le [structure](external-dicts-dict-structure.md). Ces éléments doivent contenir des éléments `name` et`type` (si `type` n'est pas spécifié, le type par défaut sera utilisé-Date). `type` peut être n'importe quel type numérique (Date / DateTime / UInt64 / Int32 / autres).

Exemple:

``` xml
<structure>
    <id>
        <name>Id</name>
    </id>
    <range_min>
        <name>first</name>
        <type>Date</type>
    </range_min>
    <range_max>
        <name>last</name>
        <type>Date</type>
    </range_max>
    ...
```

ou

``` sql
CREATE DICTIONARY somedict (
    id UInt64,
    first Date,
    last Date
)
PRIMARY KEY id
LAYOUT(RANGE_HASHED())
RANGE(MIN first MAX last)
```

Pour travailler avec ces dictionnaires, vous devez passer un argument supplémentaire à l' `dictGetT` fonction, pour laquelle une plage est sélectionnée:

``` sql
dictGetT('dict_name', 'attr_name', id, date)
```

Cette fonction retourne la valeur pour l' `id`s et la plage de dates qui inclut la date passée.

Détails de l'algorithme:

-   Si l' `id` est introuvable ou une plage n'est pas trouvé pour l' `id` il retourne la valeur par défaut pour le dictionnaire.
-   S'il y a des plages qui se chevauchent, vous pouvez en utiliser.
-   Si le délimiteur est `NULL` ou une date non valide (telle que 1900-01-01 ou 2039-01-01), la plage est laissée ouverte. La gamme peut être ouverte des deux côtés.

Exemple de Configuration:

``` xml
<yandex>
        <dictionary>

                ...

                <layout>
                        <range_hashed />
                </layout>

                <structure>
                        <id>
                                <name>Abcdef</name>
                        </id>
                        <range_min>
                                <name>StartTimeStamp</name>
                                <type>UInt64</type>
                        </range_min>
                        <range_max>
                                <name>EndTimeStamp</name>
                                <type>UInt64</type>
                        </range_max>
                        <attribute>
                                <name>XXXType</name>
                                <type>String</type>
                                <null_value />
                        </attribute>
                </structure>

        </dictionary>
</yandex>
```

ou

``` sql
CREATE DICTIONARY somedict(
    Abcdef UInt64,
    StartTimeStamp UInt64,
    EndTimeStamp UInt64,
    XXXType String DEFAULT ''
)
PRIMARY KEY Abcdef
RANGE(MIN StartTimeStamp MAX EndTimeStamp)
```

### cache {#cache}

Le dictionnaire est stocké dans un cache qui a un nombre fixe de cellules. Ces cellules contiennent des éléments fréquemment utilisés.

Lors de la recherche d'un dictionnaire, le cache est recherché en premier. Pour chaque bloc de données, toutes les clés qui ne sont pas trouvées dans le cache ou qui sont obsolètes sont demandées à la source en utilisant `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. Les données reçues sont ensuite écrites dans le cache.

Pour les dictionnaires de cache, l'expiration [vie](external-dicts-dict-lifetime.md) des données dans le cache peuvent être définies. Si plus de temps que `lifetime` passé depuis le chargement des données dans une cellule, la valeur de la cellule n'est pas utilisée et elle est demandée à nouveau la prochaine fois qu'elle doit être utilisée.
C'est la moins efficace de toutes les façons de stocker les dictionnaires. La vitesse du cache dépend fortement des paramètres corrects et que le scénario d'utilisation. Un dictionnaire de type de cache fonctionne bien uniquement lorsque les taux de réussite sont suffisamment élevés (recommandé 99% et plus). Vous pouvez afficher le taux de réussite moyen dans le `system.dictionaries` table.

Pour améliorer les performances du cache, utilisez une sous-requête avec `LIMIT`, et appelez la fonction avec le dictionnaire en externe.

Soutenu [source](external-dicts-dict-sources.md): MySQL, ClickHouse, exécutable, HTTP.

Exemple de paramètres:

``` xml
<layout>
    <cache>
        <!-- The size of the cache, in number of cells. Rounded up to a power of two. -->
        <size_in_cells>1000000000</size_in_cells>
    </cache>
</layout>
```

ou

``` sql
LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
```

Définissez une taille de cache suffisamment grande. Vous devez expérimenter pour sélectionner le nombre de cellules:

1.  Définissez une valeur.
2.  Exécutez les requêtes jusqu'à ce que le cache soit complètement plein.
3.  Évaluer la consommation de mémoire en utilisant le `system.dictionaries` table.
4.  Augmentez ou diminuez le nombre de cellules jusqu'à ce que la consommation de mémoire requise soit atteinte.

!!! warning "Avertissement"
    N'utilisez pas ClickHouse comme source, car le traitement des requêtes avec des lectures aléatoires est lent.

### complex_key_cache {#complex-key-cache}

Ce type de stockage est pour une utilisation avec composite [touches](external-dicts-dict-structure.md). Semblable à `cache`.

### direct {#direct}

Le dictionnaire n'est pas stocké dans la mémoire et va directement à la source, pendant le traitement d'une demande.

La clé du dictionnaire a le `UInt64` type.

Tous les types de [source](external-dicts-dict-sources.md), sauf les fichiers locaux, sont pris en charge.

Exemple de Configuration:

``` xml
<layout>
  <direct />
</layout>
```

ou

``` sql
LAYOUT(DIRECT())
```

### complex_key_direct {#complex-key-direct}

Ce type de stockage est destiné à être utilisé avec des [clés](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md) composites. Similaire à `direct`

### ip_trie {#ip-trie}

Ce type de stockage permet de mapper des préfixes de réseau (adresses IP) à des métadonnées telles que ASN.

Exemple: la table contient les préfixes de réseau et leur correspondant en tant que numéro et Code de pays:

``` text
  +-----------|-----|------+
  | prefix          | asn   | cca2   |
  +=================+=======+========+
  | 202.79.32.0/20  | 17501 | NP     |
  +-----------|-----|------+
  | 2620:0:870::/48 | 3856  | US     |
  +-----------|-----|------+
  | 2a02:6b8:1::/48 | 13238 | RU     |
  +-----------|-----|------+
  | 2001:db8::/32   | 65536 | ZZ     |
  +-----------|-----|------+
```

Lorsque vous utilisez ce type de mise en page, la structure doit avoir une clé composite.

Exemple:

``` xml
<structure>
    <key>
        <attribute>
            <name>prefix</name>
            <type>String</type>
        </attribute>
    </key>
    <attribute>
            <name>asn</name>
            <type>UInt32</type>
            <null_value />
    </attribute>
    <attribute>
            <name>cca2</name>
            <type>String</type>
            <null_value>??</null_value>
    </attribute>
    ...
```

ou

``` sql
CREATE DICTIONARY somedict (
    prefix String,
    asn UInt32,
    cca2 String DEFAULT '??'
)
PRIMARY KEY prefix
```

La clé ne doit avoir qu'un seul attribut de type chaîne contenant un préfixe IP autorisé. Les autres types ne sont pas encore pris en charge.

Pour les requêtes, vous devez utiliser les mêmes fonctions (`dictGetT` avec un n-uplet) comme pour les dictionnaires avec des clés composites:

``` sql
dictGetT('dict_name', 'attr_name', tuple(ip))
```

La fonction prend soit `UInt32` pour IPv4, ou `FixedString(16)` pour IPv6:

``` sql
dictGetString('prefix', 'asn', tuple(IPv6StringToNum('2001:db8::1')))
```

Les autres types ne sont pas encore pris en charge. La fonction renvoie l'attribut du préfixe correspondant à cette adresse IP. S'il y a chevauchement des préfixes, le plus spécifique est retourné.

Les données sont stockées dans une `trie`. Il doit complètement s'intégrer dans la RAM.

[Article Original](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_layout/) <!--hide-->
