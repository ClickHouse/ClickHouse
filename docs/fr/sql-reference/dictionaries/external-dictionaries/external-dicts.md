---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: "Description G\xE9n\xE9rale"
---

# Dictionnaires Externes {#dicts-external-dicts}

Vous pouvez ajouter vos propres dictionnaires à partir de diverses sources de données. La source de données d'un dictionnaire peut être un texte local ou un fichier exécutable, une ressource HTTP(S) ou un autre SGBD. Pour plus d'informations, voir “[Sources pour les dictionnaires externes](external-dicts-dict-sources.md)”.

ClickHouse:

-   Stocke entièrement ou partiellement les dictionnaires en RAM.
-   Met à jour périodiquement les dictionnaires et charge dynamiquement les valeurs manquantes. En d'autres mots, les dictionnaires peuvent être chargés dynamiquement.
-   Permet de créer des dictionnaires externes avec des fichiers xml ou [Les requêtes DDL](../../statements/create.md#create-dictionary-query).

La configuration des dictionnaires externes peut être située dans un ou plusieurs fichiers xml. Le chemin d'accès à la configuration spécifiée dans le [dictionaries\_config](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_config) paramètre.

Les dictionnaires peuvent être chargés au démarrage du serveur ou à la première utilisation, en fonction [dictionaries\_lazy\_load](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load) paramètre.

Le [dictionnaire](../../../operations/system-tables.md#system_tables-dictionaries) la table système contient des informations sur les dictionnaires configurés sur le serveur. Pour chaque dictionnaire, vous pouvez y trouver:

-   Statut du dictionnaire.
-   Paramètres de Configuration.
-   Des métriques telles que la quantité de RAM allouée pour le dictionnaire ou un certain nombre de requêtes depuis que le dictionnaire a été chargé avec succès.

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

Vous pouvez [configurer](external-dicts-dict.md) le nombre de dictionnaires dans le même fichier.

[Requêtes DDL pour les dictionnaires](../../statements/create.md#create-dictionary-query) ne nécessite aucun enregistrement supplémentaire dans la configuration du serveur. Ils permettent de travailler avec des dictionnaires en tant qu'entités de première classe, comme des tables ou des vues.

!!! attention "Attention"
    Vous pouvez convertir les valeurs pour un petit dictionnaire en le décrivant dans un `SELECT` requête (voir la [transformer](../../../sql-reference/functions/other-functions.md) fonction). Cette fonctionnalité n'est pas liée aux dictionnaires externes.

## Voir Aussi {#ext-dicts-see-also}

-   [Configuration D'un dictionnaire externe](external-dicts-dict.md)
-   [Stockage des dictionnaires en mémoire](external-dicts-dict-layout.md)
-   [Mises À Jour Du Dictionnaire](external-dicts-dict-lifetime.md)
-   [Sources de dictionnaires externes](external-dicts-dict-sources.md)
-   [Clé et champs du dictionnaire](external-dicts-dict-structure.md)
-   [Fonctions pour travailler avec des dictionnaires externes](../../../sql-reference/functions/ext-dict-functions.md)

[Article Original](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts/) <!--hide-->
