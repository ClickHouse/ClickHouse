---
description: 'Documentation de Clickhouse-disks'
sidebar_label: 'clickhouse-disks'
sidebar_position: 59
slug: /operations/utilities/clickhouse-disks
title: 'Clickhouse-disks'
doc_type: 'reference'
---

Un utilitaire offrant des opérations de type système de fichiers pour les disques ClickHouse. Il peut fonctionner aussi bien en mode interactif qu’en mode non interactif.

<div id="program-wide-options">
  ## Options globales du programme
</div>

* `--config-file, -C` -- chemin vers la configuration ClickHouse, par défaut `/etc/clickhouse-server/config.xml`.
* `--save-logs` -- journalise la progression des commandes appelées dans `/var/log/clickhouse-server/clickhouse-disks.log`.
* `--log-level` -- [type](../server-configuration-parameters/settings#logger) d’événements à journaliser, par défaut `none`.
* `--disk` -- disque à utiliser pour les commandes `mkdir, move, read, write, remove`. Par défaut : `default`.
* `--query, -q` -- requête unique pouvant être exécutée sans lancer le mode interactif
* `--help, -h` -- affiche toutes les options et commandes avec leur description

<div id="lazy-initialization">
  ## Initialisation à la demande
</div>

Tous les disques disponibles dans la configuration sont initialisés à la demande. Cela signifie que l&#39;objet correspondant à un disque n&#39;est initialisé que lorsque ce disque est utilisé dans une commande. Cela permet de rendre l&#39;utilitaire plus robuste et d&#39;éviter d&#39;accéder à des disques décrits dans la configuration mais non utilisés par l&#39;utilisateur, et dont l&#39;initialisation pourrait échouer. Cependant, un disque doit être initialisé au démarrage de clickhouse-disks. Ce disque est spécifié avec le paramètre `--disk` en ligne de commande (la valeur par défaut est `default`).

<div id="default-disks">
  ## Disques par défaut
</div>

Après le démarrage, deux disques non spécifiés dans la configuration sont disponibles pour l’initialisation.

1. **Disque `local`** : ce disque est conçu pour reproduire le système de fichiers local depuis lequel l’utilitaire `clickhouse-disks` a été lancé. Son chemin initial est le répertoire depuis lequel `clickhouse-disks` a été démarré, et il est monté à la racine du système de fichiers.

2. **Disque `default`** : ce disque est monté sur le système de fichiers local dans le répertoire spécifié par le paramètre `clickhouse/path` de la configuration (la valeur par défaut est `/var/lib/clickhouse`). Son chemin initial est défini sur `/`.

<div id="clickhouse-disks-state">
  ## État de clickhouse-disks
</div>

Pour chaque disque ajouté, l’utilitaire conserve le répertoire courant (comme dans un système de fichiers classique). L’utilisateur peut changer de répertoire courant et passer d’un disque à l’autre.

L’état se reflète dans une invite de commande &quot;`disk_name`:`path_name`&quot;

<div id="commands">
  ## Commandes
</div>

Dans ce fichier de documentation, tous les arguments positionnels obligatoires sont notés `<parameter>` et les arguments nommés sont notés `[--parameter value]`. Tous les paramètres positionnels peuvent également être mentionnés comme paramètres nommés à l&#39;aide du nom correspondant.

* `cd (change-dir, change_dir) [--disk disk] <path>`
  Changer de répertoire vers le chemin `path` sur le disque `disk` (la valeur par défaut est le disque actuel). Aucun changement de disque n&#39;est effectué.
* `copy (cp) [--disk-from disk_1] [--disk-to disk_2] <path-from> <path-to>`.
  Copier récursivement les données depuis `path-from` sur le disque `disk_1` (la valeur par défaut est le disque actuel, paramètre `disk` en mode non interactif)
  vers `path-to` sur le disque `disk_2` (la valeur par défaut est le disque actuel, paramètre `disk` en mode non interactif).
* `current_disk_with_path (current, current_disk, current_path)`
  Afficher l&#39;état actuel au format :
  `Disk: "current_disk" Path: "current path on current disk"`
* `du [--human-readable] [<path>]`
  Afficher la taille totale en octets du fichier ou du répertoire situé à `path` sur le disque actuel. Pour un répertoire, la taille de tous les fichiers qu&#39;il contient est additionnée récursivement. Si `path` n&#39;est pas spécifié, le répertoire actuel est utilisé. Avec `--human-readable` (`-h`), la taille est affichée dans un format lisible par l&#39;humain (par ex. `1.23 GiB`).
* `help [<command>]`
  Afficher le message d&#39;aide pour la commande `command`. Si `command` n&#39;est pas spécifiée, afficher des informations sur toutes les commandes.
* `move (mv) <path-from> <path-to>`.
  Déplacer un fichier ou un répertoire de `path-from` vers `path-to` sur le disque actuel.
* `remove (rm, delete) <path>`.
  Supprimer `path` récursivement sur le disque actuel.
* `link (ln) <path-from> <path-to>`.
  Créer un lien physique de `path-from` vers `path-to` sur le disque actuel.
* `list (ls) [--recursive] <path>`
  Lister les fichiers dans `path` sur le disque actuel. Non récursif par défaut.
* `list-disks (list_disks, ls-disks, ls_disks)`.
  Lister les noms des disques.
* `mkdir [--recursive] <path>` on a current disk.
  Créer un répertoire sur le disque actuel. Non récursif par défaut.
* `read (r) <path-from> [--path-to path]`
  Lire un fichier depuis `path-from` vers `path` (`stdout` si non fourni).
* `read-bitmap <path-from> [--values]`
  Inspecter un fichier sidecar delete-bitmap (`.rbm`) à `path-from`. Affiche le magic et la version, la validité du CRC, la cardinalité (nombre de lignes supprimées) et la plage de lignes. Avec `--values`, affiche également tous les bits activés (les offsets des lignes supprimées) par ordre croissant.
* `switch-disk [--path path] <disk>`
  Basculer vers le disque `disk` sur le chemin `path` (si `path` n&#39;est pas spécifié, la valeur par défaut est le chemin précédent sur le disque `disk`).
* `write (w) [--path-from path] <path-to>`.
  Écrire un fichier depuis `path` (`stdin` si `path` n&#39;est pas fourni, l&#39;entrée doit se terminer par Ctrl+D) vers `path-to`.
* `wc <path> [--bytes] [--lines] [--words]`
  Compter les octets, les lignes et les mots dans le fichier à `path` sur le disque actuel (comme Unix `wc`). Sans option, les trois décomptes sont affichés dans l&#39;ordre suivant : lignes, mots, puis octets. Utilisez `--bytes` (`-c`), `--lines` (`-l`), `--words` (`-w`) pour sélectionner des décomptes spécifiques.
* `sed <expression> <path>`
  Appliquer l&#39;`expression` `sed` au fichier à `path` sur le disque actuel, en place. Nécessite que `sed` soit installé sur l&#39;hôte. Une seule expression `sed` sans options est prise en charge (par ex. `'s/foo/bar/g'`, `'/foo/d'`), pas plusieurs expressions (`-e ... -e ...`) ni des options combinées avec une adresse (par ex. `-n` avec `4,10p`).
* `read-checksums <path>`
  Lire un fichier `checksums.txt` d&#39;une partie de données `MergeTree` sur le disque actuel et l&#39;afficher dans `stdout` sous forme de tableau lisible par l&#39;humain, séparé par des tabulations, avec les colonnes `name`, `file_size`, `file_hash`, `uncompressed_size` et `uncompressed_hash`. Les deux dernières colonnes ne sont présentes que pour les fichiers compressés.