---
description: 'Documentation de l’interface web intégrée de recherche dans la documentation, disponible au chemin `/docs` de l’interface HTTP et s’appuyant sur la table `system.documentation`'
sidebar_label: 'Recherche dans la documentation'
sidebar_position: 23
slug: /interfaces/documentation-search
title: 'Recherche dans la documentation'
doc_type: 'reference'
---

La page de recherche dans la documentation est une petite interface web autonome qui permet d’effectuer des recherches instantanées dans la documentation de référence intégrée. Elle est disponible sur n’importe quel port HTTP de ClickHouse, au chemin `/docs`.

Accédez à `/docs` sur n’importe quel port HTTP de ClickHouse (par exemple, `http://localhost:8123/docs`) pour l’ouvrir.

<div id="what-it-does">
  ## Ce qu’elle fait
</div>

La page interroge la table [`system.documentation`](/fr/operations/system-tables/documentation) via HTTP au fil de la saisie, et affiche le Markdown de l’entité sélectionnée. Comme elle lit `system.documentation`, elle couvre toutes les entités exposées par cette table — fonctions, fonctions d’agrégation, fonctions de table, moteurs de table, moteurs de base de données, types de données, paramètres, formats, codecs de compression, événements de profil, métriques, tables système elles-mêmes, et plus encore — et correspond toujours à la documentation intégrée au serveur en cours d’exécution.

Saisissez un terme dans la zone de recherche et les résultats apparaissent dans une liste à code couleur selon le type ; la sélection d’un résultat affiche sa documentation. Le rendu inclut :

* un lien en forme de crayon à côté du titre de l’entité, qui ouvre son fichier source sur GitHub, à partir de la colonne `source` de `system.documentation` ;
* la coloration syntaxique ClickHouse SQL des blocs de code, à l’aide du même lexer intégré (`Lexer.wasm`) que l’interface [`/play`](/fr/interfaces/http) ;
* les formules TeX via [KaTeX](https://katex.org/) (par exemple, la formule de la page `corr`) ;
* les encadrés `:::note`/`:::tip`/…, les ancres de titre avec des liens partageables, et un bouton « Copier » au survol des blocs de code ;
* les liens relatifs résolus dans l’application vers une autre entité documentée lorsqu’elle existe, sinon vers `https://clickhouse.com/docs` ; les références « Related » et « Alias of » deviennent des liens internes à l’application.

Le terme de recherche actuel, l’entité ouverte et la section sont reflétés dans le fragment d’URL, de sorte qu’une page ou une section précise puisse être liée directement et soit restaurée par la navigation précédent/suivant du navigateur. Un sélecteur de thème clair/sombre (avec détection automatique) s’aligne sur `/play`.

<div id="connecting">
  ## Connexion
</div>

L’en-tête comporte des champs `URL`, `user` et `password`, exactement comme dans `/play`. Lorsque la page est servie par ClickHouse, l’`URL` correspond par défaut à l’origine actuelle ; lorsque la page est ouverte en tant que fichier local, elle vaut par défaut `http://localhost:8123/`, ce qui permet aussi d’ouvrir la page localement pour se connecter à un serveur distant. Le cache des noms de liens croisés est reconstruit automatiquement lorsque la connexion change.

<div id="assets">
  ## Ressources
</div>

Toutes les ressources — y compris le moteur de rendu Markdown ([Marked](https://marked.js.org/)), le moteur de rendu mathématique (KaTeX, avec ses polices) et l’analyseur lexical SQL — sont servies directement par le binaire ClickHouse lui-même lorsque la page est servie via HTTP. Aucun CDN tiers n’est chargé depuis l’origine HTTP de ClickHouse ; la page est donc autonome, fonctionne hors ligne et n’exécute pas de code réseau tiers en même temps que les identifiants qu’elle traite.

<div id="security">
  ## Considérations de sécurité
</div>

La page envoie des requêtes à l’endpoint HTTP de ClickHouse avec les identifiants saisis dans l’en-tête ; les mêmes mises en garde que pour le protocole HTTP s’appliquent donc ici :

* Servez toujours `/docs` via HTTPS dans les environnements non fiables afin de protéger les identifiants.
* Restreignez l’accès au niveau du réseau (pare-feu, proxy inverse ou configuration `listen_host`) de la même manière que pour le protocole HTTP.

`system.documentation` contient uniquement la documentation de référence statique intégrée au serveur ; la page n’expose donc aucune donnée de vos tables.