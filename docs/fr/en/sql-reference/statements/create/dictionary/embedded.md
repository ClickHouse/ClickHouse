---
description: 'Dictionnaires geobase intégrés dans ClickHouse'
sidebar_label: 'Dictionnaires intégrés'
sidebar_position: 6
slug: /sql-reference/statements/create/dictionary/embedded
title: 'Dictionnaires intégrés (geobase)'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

ClickHouse intègre une fonctionnalité permettant de travailler avec une géobase.

Elle vous permet de :

* Utiliser l’ID d’une région pour obtenir son nom dans la langue souhaitée.
* Utiliser l’ID d’une région pour obtenir l’ID d’une ville, d’une zone, d’un district fédéral, d’un pays ou d’un continent.
* Vérifier si une région fait partie d’une autre région.
* Obtenir la chaîne des régions parentes.

Toutes les fonctions prennent en charge la « translocalité », c’est-à-dire la possibilité d’utiliser simultanément différentes perspectives sur l’appartenance des régions. Pour plus d’informations, consultez la section « Fonctions pour travailler avec les dictionnaires de web analytics ».

Les dictionnaires internes sont désactivés dans le paquet par défaut.
Pour les activer, décommentez les paramètres `path_to_regions_hierarchy_file` et `path_to_regions_names_files` dans le fichier de configuration du serveur.

La géobase est chargée à partir de fichiers texte.

Placez les fichiers `regions_hierarchy*.txt` dans le répertoire `path_to_regions_hierarchy_file`. Ce paramètre de configuration doit contenir le chemin vers le fichier `regions_hierarchy.txt` (la hiérarchie régionale par défaut), et les autres fichiers (`regions_hierarchy_ua.txt`) doivent se trouver dans le même répertoire.

Placez les fichiers `regions_names_*.txt` dans le répertoire `path_to_regions_names_files`.

Vous pouvez également créer ces fichiers vous-même. Le format des fichiers est le suivant :

`regions_hierarchy*.txt` : TabSeparated (sans en-tête), colonnes :

* ID de région (`UInt32`)
* ID de la région parente (`UInt32`)
* type de région (`UInt8`) : 1 - continent, 3 - pays, 4 - district fédéral, 5 - région, 6 - ville ; les autres types n’ont pas de valeur
* population (`UInt32`) — colonne facultative

`regions_names_*.txt` : TabSeparated (sans en-tête), colonnes :

* ID de région (`UInt32`)
* nom de la région (`String`) — Ne peut pas contenir de tabulations ni de sauts de ligne, même échappés.

Un tableau plat est utilisé pour le stockage en RAM. C’est pourquoi les ID ne doivent pas dépasser un million.

Les dictionnaires peuvent être mis à jour sans redémarrer le serveur. Cependant, l’ensemble des dictionnaires disponibles n’est pas mis à jour.
Pour les mises à jour, les dates de modification des fichiers sont vérifiées. Si un fichier a changé, le dictionnaire est mis à jour.
L’intervalle de vérification des modifications est configuré dans le paramètre `builtin_dictionaries_reload_interval`.
Les mises à jour des dictionnaires (autres que le chargement lors de la première utilisation) ne bloquent pas les requêtes. Pendant les mises à jour, les requêtes utilisent les anciennes versions des dictionnaires. Si une erreur se produit lors d’une mise à jour, elle est consignée dans le journal du serveur, et les requêtes continuent d’utiliser l’ancienne version des dictionnaires.

Nous vous recommandons de mettre périodiquement à jour les dictionnaires à partir de la géobase. Lors d’une mise à jour, générez de nouveaux fichiers et écrivez-les dans un emplacement distinct. Quand tout est prêt, renommez-les pour qu’ils remplacent les fichiers utilisés par le serveur.

Il existe également des fonctions pour travailler avec les identifiants d’OS et les moteurs de recherche, mais il ne faut pas les utiliser.