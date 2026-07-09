---
description: "Documentation de l'UI SQL Web (Play), l'interface de requête intégrée au navigateur, accessible via `/play`"
sidebar_label: 'UI SQL Web'
sidebar_position: 21
slug: /interfaces/web-sql
title: 'UI SQL Web (Play)'
doc_type: 'reference'
---

L&#39;UI SQL Web (Play) est l&#39;interface de requête intégrée à ClickHouse, accessible dans le navigateur. Elle est servie sur n&#39;importe quel port HTTP de ClickHouse au chemin `/play` (par exemple, `http://localhost:8123/play`). Elle vous permet d&#39;écrire et d&#39;exécuter des requêtes, d&#39;afficher les résultats sous forme de table ou de graphique, et de partager une requête en copiant son URL.

L&#39;ensemble de l&#39;interface se trouve dans `programs/server/play.html`, une page autonome unique servie directement par le binaire ClickHouse, sans framework ni étape de build. La seule exception concerne le rendu des graphiques : la bibliothèque de graphiques `uPlot` est chargée à la demande depuis un CDN tiers la première fois qu&#39;un résultat est affiché sous forme de graphique. Les graphiques ne sont donc pas disponibles dans les déploiements hors ligne ou dans lesquels le trafic sortant est restreint.

<div id="query-tabs">
  ## Onglets de requête
</div>

Les onglets vous permettent de garder plusieurs requêtes côte à côte, au lieu de jongler avec elles dans un seul éditeur ou de vous en remettre à l’historique du navigateur.

Chaque onglet possède son propre texte de requête, son titre, ses paramètres de requête et son dernier résultat. Les paramètres de connexion (URL, utilisateur, mot de passe) restent globaux et sont partagés par tous les onglets.

<div id="when-the-tab-bar-appears">
  ### Quand la barre d’onglets apparaît
</div>

La barre d’onglets apparaît dès qu’une requête a été exécutée ou dès qu’il y a plus d’un onglet. Un onglet unique sans résultat a exactement le même aspect que la page avant l’introduction des onglets ; la barre d’onglets ne s’affiche donc que lorsque vous en avez besoin.

L’onglet actif se fond visuellement dans la page : son arrière-plan reprend la couleur de hachage associée à la requête (la même que celle déjà utilisée pour l’arrière-plan de la page), avec un dégradé plus saturé en haut dans le thème clair et plus lumineux en haut dans le thème sombre. Les onglets inactifs sont teintés selon le hachage du texte de leur propre requête, ce qui permet de les distinguer automatiquement par la couleur.

<div id="creating-closing-and-renaming-tabs">
  ### Créer, fermer et renommer des onglets
</div>

* Créez un nouvel onglet à l’aide du bouton `[+]` à droite des onglets.
* Fermez un onglet à l’aide de l’icône `x` sur l’onglet.
* Les nouveaux onglets reçoivent les noms par défaut `Query A`, `Query B`, etc.
* Cliquez sur le titre de l’onglet actif pour le modifier directement ; le champ d’édition s’agrandit pour s’adapter au texte.

<div id="switching-tabs">
  ### Passer d’un onglet à l’autre
</div>

* Cliquez sur un onglet inactif pour l’activer.
* Faites tourner la molette de la souris au-dessus du panneau d’onglets pour passer d’un onglet à l’autre : vers le haut, vous passez à l’onglet de gauche ; vers le bas, à l’onglet de droite (s’ils existent). Le défilement vertical comme horizontal de la molette fonctionne.

La barre d’onglets est fixe horizontalement : elle reste à gauche lors du défilement horizontal de la page, comme le logo ClickHouse en bas, et défile verticalement avec le reste de la page.

<div id="persistence-and-browser-history">
  ### Persistance et historique du navigateur
</div>

L’espace de travail — les onglets, leurs titres, l’onglet actif, leur ordre et de petits instantanés des résultats — est enregistré dans IndexedDB et restauré lors du rechargement. La persistance fonctionne au mieux : si IndexedDB n’est pas disponible, l’espace de travail bascule vers un état en mémoire pour la session en cours.

Les onglets s’intègrent également à l’API History du navigateur et à l’URL :

* L’état de l’historique conserve l’onglet actif, de sorte que les boutons précédent et suivant du navigateur permettent de changer d’onglet.
* L’URL reçoit un paramètre `tab=<name>`. Au chargement, la requête de l’URL et le paramètre `tab` sont rapprochés des onglets enregistrés : un onglet existant portant ce nom est réutilisé (et sa requête est remplacée), ou un nouvel onglet est créé si ce nom est introuvable ou s’il n’était pas nommé. Cela permet d’ouvrir une URL avec une nouvelle requête tout en conservant vos propres onglets enregistrés.

<div id="limitations">
  ### Limites
</div>

Changer d’onglet pendant l’exécution d’une requête fait perdre l’état d’exécution de cette requête.

Seuls les petits résultats sont sauvegardés dans un instantané en vue d’une restauration. Un résultat volumineux (au-delà de la limite de taille de l’instantané) ou un résultat sous forme d’image n’est pas conservé : après un changement d’onglet ou un rechargement, l’onglet conserve sa requête, mais pas le résultat affiché, et il suffit de relancer la requête pour le reproduire. Cela s’applique aussi bien au résultat d’une seule requête qu’à la sortie combinée d’une exécution « Run all » (multi-requête).