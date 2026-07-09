---
description: 'Documentation de l’API HTTP et du tableau de bord web intégré de ClickHouse Keeper'
sidebar_label: 'API HTTP de Keeper'
sidebar_position: 70
slug: /operations/utilities/clickhouse-keeper-http-api
title: 'API HTTP et tableau de bord de Keeper'
doc_type: 'reference'
---

ClickHouse Keeper fournit une API HTTP et un tableau de bord web intégré pour la surveillance, les vérifications d’état et la gestion du stockage.
Cette interface permet aux opérateurs de consulter l’état du cluster, d’exécuter des commandes et de gérer le stockage de Keeper via un navigateur web ou des clients HTTP.

<div id="configuration">
  ## Configuration
</div>

Pour activer l’API HTTP, ajoutez la section `http_control` dans votre configuration `keeper_server` :

```xml
<keeper_server>
    <!-- Other keeper_server configuration -->

    <http_control>
        <port>9182</port>
        <!-- <secure_port>9443</secure_port> -->
    </http_control>
</keeper_server>
```

<div id="configuration-options">
  ### Options de configuration
</div>

| Paramètre                                 | Par défaut | Description                                                 |
| ----------------------------------------- | ---------- | ----------------------------------------------------------- |
| `http_control.port`                       | -          | Port HTTP du tableau de bord et de l’API                    |
| `http_control.secure_port`                | -          | Port HTTPS (nécessite une configuration SSL)                |
| `http_control.readiness.endpoint`         | `/ready`   | Chemin personnalisé pour la sonde de readiness              |
| `http_control.storage.session_timeout_ms` | `30000`    | Timeout de session pour les opérations de l’API de stockage |

<div id="endpoints">
  ## Endpoints
</div>

<div id="dashboard">
  ### Tableau de bord
</div>

* **Chemin**: `/dashboard`
* **Méthode**: GET
* **Description**: Expose un tableau de bord web intégré pour surveiller et gérer Keeper

Le tableau de bord offre :

* Une visualisation en temps réel de l’état du cluster
* La surveillance des nœuds (rôle, latence, connexions)
* Un explorateur de stockage
* Une interface d’exécution de commandes

<div id="readiness-probe">
  ### Sonde de disponibilité
</div>

* **Chemin** : `/ready` (configurable)
* **Méthode** : GET
* **Description** : endpoint de contrôle de l’état de santé

Réponse en cas de succès (HTTP 200) :

```json
{
  "status": "ok",
  "details": {
    "role": "leader",
    "hasLeader": true
  }
}
```

<div id="commands-api">
  ### API de commandes
</div>

* **Chemin** : `/api/v1/commands/{command}`
* **Méthodes** : GET, POST
* **Description** : Exécute des commandes Four-Letter Word ou des commandes CLI du client ClickHouse Keeper

Paramètres de requête :

* `command` - La commande à exécuter
* `cwd` - Répertoire de travail courant pour les commandes utilisant un chemin (par défaut : `/`)

Exemples :

```bash
# Four-Letter Word command
curl http://localhost:9182/api/v1/commands/stat

# ZooKeeper CLI command
curl "http://localhost:9182/api/v1/commands/ls?command=ls%20'/'&cwd=/"
```

<div id="storage-api">
  ### API de stockage
</div>

* **Chemin de base**: `/api/v1/storage`
* **Description**: API REST pour les opérations de stockage de Keeper

L’API de stockage suit les conventions REST, dans lesquelles les méthodes HTTP indiquent le type d’opération :

| Opération     | Chemin                                 | Méthode | Code d’état | Description                       |
| ------------- | -------------------------------------- | ------- | ----------- | --------------------------------- |
| Obtenir       | `/api/v1/storage/{path}`               | GET     | 200         | Obtenir les données du nœud       |
| Lister        | `/api/v1/storage/{path}?children=true` | GET     | 200         | Lister les nœuds enfants          |
| Existence     | `/api/v1/storage/{path}`               | HEAD    | 200         | Vérifier si le nœud existe        |
| Créer         | `/api/v1/storage/{path}`               | POST    | 201         | Créer un nouveau nœud             |
| Mettre à jour | `/api/v1/storage/{path}?version={v}`   | PUT     | 200         | Mettre à jour les données du nœud |
| Supprimer     | `/api/v1/storage/{path}?version={v}`   | DELETE  | 204         | Supprimer le nœud                 |