---
machine_translated: true
---

# Dictionnaires Externes {#dicts-external-dicts}

Vous pouvez ajouter vos propres dictionnaires à partir de diverses sources de données. La source de données d'un dictionnaire peut être un texte local ou un fichier exécutable, une ressource HTTP(S) ou un autre SGBD. Pour plus d'informations, voir “[Sources pour les dictionnaires externes](external_dicts_dict_sources.md)”.

ClickHouse:

-   Stocke entièrement ou partiellement les dictionnaires en RAM.
-   Met à jour périodiquement les dictionnaires et charge dynamiquement les valeurs manquantes. En d'autres mots, les dictionnaires peuvent être chargés dynamiquement.
-   Permet de créer des dictionnaires externes avec des fichiers xml ou [Les requêtes DDL](../create.md#create-dictionary-query).

La configuration des dictionnaires externes peut être située dans un ou plusieurs fichiers xml. Le chemin d'accès à la configuration spécifiée dans le [dictionaries\_config](../../operations/server_settings/settings.md#server_settings-dictionaries_config) paramètre.

Les dictionnaires peuvent être chargés au démarrage du serveur ou à la première utilisation, en fonction [dictionaries\_lazy\_load](../../operations/server_settings/settings.md#server_settings-dictionaries_lazy_load) paramètre.

Le fichier de configuration du dictionnaire a le format suivant:

``` xml
<yandex>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of <dictionary> sections in the configuration file. -->
    </dictionary>

</yandex>
```

Vous pouvez [configurer](external_dicts_dict.md) le nombre de dictionnaires dans le même fichier.

[Requêtes DDL pour les dictionnaires](../create.md#create-dictionary-query) ne nécessite aucun enregistrement supplémentaire dans la configuration du serveur. Ils permettent de travailler avec des dictionnaires en tant qu'entités de première classe, comme des tables ou des vues.

!!! attention "Attention"
    Vous pouvez convertir les valeurs pour un petit dictionnaire en le décrivant dans un `SELECT` requête (voir la [transformer](../functions/other_functions.md) fonction). Cette fonctionnalité n'est pas liée aux dictionnaires externes.

## Voir aussi {#ext-dicts-see-also}

-   [Configuration D'un dictionnaire externe](external_dicts_dict.md)
-   [Stockage des dictionnaires en mémoire](external_dicts_dict_layout.md)
-   [Mises À Jour Du Dictionnaire](external_dicts_dict_lifetime.md)
-   [Sources de dictionnaires externes](external_dicts_dict_sources.md)
-   [Clé et champs du dictionnaire](external_dicts_dict_structure.md)
-   [Fonctions pour travailler avec des dictionnaires externes](../functions/ext_dict_functions.md)

[Article Original](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts/) <!--hide-->
