---
description: 'mécanisme de mise en cache permettant de stocker des 
données dans la mémoire du processus plutôt que de s’appuyer sur le cache de pages de l’OS.'
sidebar_label: 'Cache de pages en espace utilisateur'
sidebar_position: 65
slug: /operations/userspace-page-cache
title: 'Cache de pages en espace utilisateur'
doc_type: 'référence'
---

<div id="overview">
  ## Aperçu
</div>

> Le cache de pages en espace utilisateur est un nouveau mécanisme de mise en cache qui permet de stocker les données dans la mémoire du processus, au lieu de s’appuyer sur le cache de pages du système d’exploitation.

ClickHouse propose déjà le [cache du système de fichiers](/fr/docs/operations/storing-data)
pour mettre en cache les données stockées dans un service de stockage d’objets distant tel qu’Amazon S3, Google
Cloud Storage (GCS) ou Azure Blob Storage. Le cache de pages en espace utilisateur est conçu
pour accélérer l’accès aux données distantes lorsque la mise en cache habituelle du système d’exploitation n’est pas
assez efficace.

Il se distingue du cache du système de fichiers de la manière suivante :

| Cache du système de fichiers                                                                      | Cache de pages en espace utilisateur             |
| ------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| Écrit les données dans le système de fichiers local                                               | Présent uniquement en mémoire                    |
| Occupe de l’espace disque (également configurable sur tmpfs)                                      | Indépendant du système de fichiers               |
| Survit aux redémarrages du serveur                                                                | Ne survit pas aux redémarrages du serveur        |
| N’apparaît pas dans la consommation mémoire du serveur                                            | Apparaît dans la consommation mémoire du serveur |
| Adapté aussi bien au stockage sur disque qu’en mémoire (cache de pages du système d’exploitation) | **Bien adapté aux serveurs sans disque**         |

<div id="configuration-settings-and-usage">
  ## Paramètres de configuration et utilisation
</div>

<div id="usage">
  ### Utilisation
</div>

Pour activer le cache de pages en espace utilisateur, commencez par le configurer sur le serveur :

```bash
cat config.d/page_cache.yaml
page_cache_max_size: 100G
```

:::note
Le cache de pages en espace utilisateur utilisera jusqu&#39;à la quantité de mémoire spécifiée, mais
cette quantité de mémoire n&#39;est pas réservée. Cette mémoire sera libérée lorsqu&#39;elle sera nécessaire
pour d&#39;autres besoins du serveur.
:::

Ensuite, activez son utilisation au niveau des requêtes :

```sql
SET use_page_cache_for_disks_without_file_cache=1;
```

<div id="settings">
  ### Paramètres
</div>

| Setting                                                 | Description                                                                                                                                                                                                                                                                                                                                                                                                | Default     |
| ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `use_page_cache_for_disks_without_file_cache`           | Utilise le cache de pages en espace utilisateur pour les disques distants pour lesquels le cache du système de fichiers n’est pas activé.                                                                                                                                                                                                                                                                  | `0`         |
| `use_page_cache_with_distributed_cache`                 | Utilise le cache de pages en espace utilisateur lorsque le Distributed Cache est utilisé.                                                                                                                                                                                                                                                                                                                  | `0`         |
| `read_from_page_cache_if_exists_otherwise_bypass_cache` | Utilise le cache de pages en espace utilisateur en mode passif, de manière similaire à [`read_from_filesystem_cache_if_exists_otherwise_bypass_cache`](/fr/docs/operations/settings/settings#read_from_filesystem_cache_if_exists_otherwise_bypass_cache).                                                                                                                                                    | `0`         |
| `page_cache_inject_eviction`                            | Le cache de pages en espace utilisateur invalide parfois certaines pages de manière aléatoire. Destiné aux tests.                                                                                                                                                                                                                                                                                          | `0`         |
| `page_cache_block_size`                                 | Taille des fragments de fichier à stocker dans le cache de pages en espace utilisateur, en octets. Toutes les lectures qui passent par le cache seront arrondies au multiple supérieur de cette taille.                                                                                                                                                                                                    | `1048576`   |
| `page_cache_history_window_ms`                          | Délai avant que la mémoire libérée puisse être utilisée par le cache de pages en espace utilisateur.                                                                                                                                                                                                                                                                                                       | `1000`      |
| `page_cache_policy`                                     | Nom de la politique du cache de pages en espace utilisateur.                                                                                                                                                                                                                                                                                                                                               | `SLRU`      |
| `page_cache_size_ratio`                                 | Taille de la file protégée dans le cache de pages en espace utilisateur, par rapport à la taille totale du cache.                                                                                                                                                                                                                                                                                          | `0.5`       |
| `page_cache_min_size`                                   | Taille minimale du cache de pages en espace utilisateur.                                                                                                                                                                                                                                                                                                                                                   | `104857600` |
| `page_cache_max_size`                                   | Taille maximale du cache de pages en espace utilisateur. Définissez-la sur 0 pour désactiver le cache. Si elle est supérieure à page&#95;cache&#95;min&#95;size, la taille du cache sera ajustée en continu dans cette plage afin d’utiliser la majeure partie de la mémoire disponible tout en maintenant l’utilisation totale de la mémoire sous la limite (`max_server_memory_usage`[`_to_ram_ratio`]). | `0`         |
| `page_cache_free_memory_ratio`                          | Fraction de la limite de mémoire à laisser libre du cache de pages en espace utilisateur. Analogue au paramètre Linux min&#95;free&#95;kbytes.                                                                                                                                                                                                                                                             | `0.15`      |
| `page_cache_lookahead_blocks`                           | En cas de cache miss dans le cache de pages en espace utilisateur, lit jusqu’à ce nombre de blocs consécutifs à la fois depuis le stockage sous-jacent, s’ils ne sont pas non plus dans le cache. Chaque bloc correspond à page&#95;cache&#95;block&#95;size octets.                                                                                                                                       | `16`        |
| `page_cache_shards`                                     | Répartit le cache de pages en espace utilisateur sur ce nombre de segments afin de réduire la contention sur les mutex. Experimental, cela n’améliore probablement pas les performances.                                                                                                                                                                                                                   | `4`         |

<div id="related-content">
  ## Contenu associé
</div>

* [Cache du système de fichiers](/fr/docs/operations/storing-data)
* [Webinaire ClickHouse v25.3](https://www.youtube.com/live/iCKEzp0_Z2Q?feature=shared\&t=1320)