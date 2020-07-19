---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: Dictionnaires Internes
---

# Dictionnaires Internes {#internal_dicts}

ClickHouse contient une fonction intégrée pour travailler avec une géobase.

Cela vous permet de:

-   Utilisez L'ID d'une région pour obtenir son nom dans la langue souhaitée.
-   Utilisez L'ID d'une région pour obtenir L'ID d'une ville, d'une région, d'un district fédéral, d'un pays ou d'un continent.
-   Vérifiez si une région fait partie d'une autre région.
-   Obtenez une chaîne de régions parentes.

Toutes les fonctions prennent en charge “translocality,” la capacité d'utiliser simultanément différentes perspectives sur la propriété de la région. Pour plus d'informations, consultez la section “Functions for working with Yandex.Metrica dictionaries”.

Les dictionnaires internes sont désactivés dans le package par défaut.
Pour les activer, décommentez les paramètres `path_to_regions_hierarchy_file` et `path_to_regions_names_files` dans le fichier de configuration du serveur.

La géobase est chargée à partir de fichiers texte.

Place de la `regions_hierarchy*.txt` les fichiers dans le `path_to_regions_hierarchy_file` répertoire. Ce paramètre de configuration doit contenir le chemin `regions_hierarchy.txt` fichier (la hiérarchie régionale par défaut), et les autres fichiers (`regions_hierarchy_ua.txt`) doit être situé dans le même répertoire.

Mettre le `regions_names_*.txt` les fichiers dans le `path_to_regions_names_files` répertoire.

Vous pouvez également créer ces fichiers vous-même. Le format de fichier est le suivant:

`regions_hierarchy*.txt`: TabSeparated (pas d'en-tête), colonnes:

-   région de l'ID (`UInt32`)
-   ID de région parent (`UInt32`)
-   type de région (`UInt8`): 1-continent, 3-pays, 4-district fédéral, 5-région, 6-ville; les autres types n'ont pas de valeurs
-   population (`UInt32`) — optional column

`regions_names_*.txt`: TabSeparated (pas d'en-tête), colonnes:

-   région de l'ID (`UInt32`)
-   nom de la région (`String`) — Can't contain tabs or line feeds, even escaped ones.

Un tableau plat est utilisé pour stocker dans la RAM. Pour cette raison, les ID ne devraient pas dépasser un million.

Les dictionnaires peuvent être mis à jour sans redémarrer le serveur. Cependant, l'ensemble des dictionnaires n'est pas mis à jour.
Pour les mises à jour, les temps de modification du fichier sont vérifiés. Si un fichier a été modifié, le dictionnaire est mis à jour.
L'intervalle de vérification des modifications est configuré dans le `builtin_dictionaries_reload_interval` paramètre.
Les mises à jour du dictionnaire (autres que le chargement lors de la première utilisation) ne bloquent pas les requêtes. Lors des mises à jour, les requêtes utilisent les anciennes versions des dictionnaires. Si une erreur se produit pendant une mise à jour, l'erreur est écrite dans le journal du serveur et les requêtes continuent d'utiliser l'ancienne version des dictionnaires.

Nous vous recommandons de mettre à jour périodiquement les dictionnaires avec la géobase. Lors d'une mise à jour, générez de nouveaux fichiers et écrivez-les dans un emplacement séparé. Lorsque tout est prêt, renommez - les en fichiers utilisés par le serveur.

Il existe également des fonctions pour travailler avec les identifiants du système d'exploitation et Yandex.Moteurs de recherche Metrica, mais ils ne devraient pas être utilisés.

[Article Original](https://clickhouse.tech/docs/en/query_language/dicts/internal_dicts/) <!--hide-->
