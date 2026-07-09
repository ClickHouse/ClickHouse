---
description: 'Documentation des modes de codage couleur par colonne dans la Web UI SQL intégrée (`/play`)'
sidebar_label: 'Codage couleur de la Web UI'
sidebar_position: 23
slug: /interfaces/web-ui-color-coding
title: 'Codage couleur de la Web UI'
doc_type: 'référence'
sidebar: false
---

La Web UI SQL intégrée (`play.html`, accessible via le chemin [`/play`](/fr/interfaces/http) sur n’importe quel port HTTP de ClickHouse) peut colorer les cellules de résultat pour faciliter le repérage, d’un coup d’œil, des motifs dans une colonne. Chaque colonne dispose de son propre mode de codage couleur, que vous pouvez activer ou désactiver indépendamment.

<div id="switching-the-mode">
  ## Changer de mode
</div>

Une icône 🌈 apparaît à droite de chaque en-tête de colonne. Cliquez dessus pour faire défiler les modes disponibles de la colonne. Sur les appareils dotés d’un périphérique de pointage avec survol (une souris), l’icône n’est affichée que lorsque l’en-tête est survolé, afin de rester discrète le reste du temps ; sur les appareils tactiles et autres appareils à pointeur grossier, qui ne prennent pas en charge le survol, l’icône est toujours affichée pour pouvoir être touchée directement.

L’ensemble des modes proposés par une colonne dépend de son type :

* Les colonnes numériques et les colonnes `Date`/`DateTime`/`Date32`/`DateTime64` défilent selon la séquence `bar` → `heatmap` → `categorical` → `none`.
* Toutes les autres colonnes basculent entre `none` et `categorical`.

Le mode par défaut est `bar` pour les colonnes numériques et `none` pour toutes les autres colonnes, y compris les colonnes de date et d’heure.

<div id="modes">
  ## Modes
</div>

* **`bar`** — dessine une barre horizontale dans la cellule, proportionnelle à la valeur. Pour les colonnes numériques, la barre part d’une ligne de base à zéro ; pour les colonnes `Date`/`DateTime`, elle couvre à la place la plage `min`..`max` de la colonne, car une ligne de base à zéro n’a pas de sens pour les timestamps.
* **`heatmap`** — remplit tout l’arrière-plan de la cellule avec une couleur qui représente la valeur mise à l’échelle entre le minimum et le maximum de la colonne.
* **`categorical`** — remplit l’arrière-plan de la cellule avec une couleur dérivée d’un hachage de la valeur de la cellule, afin que les valeurs identiques aient la même couleur et les valeurs différentes des couleurs différentes. Cela fonctionne pour n’importe quel type de colonne.
* **`none`** — aucun codage couleur.

Les colonnes `Date`, `DateTime`, `Date32` et `DateTime64` sont colorées selon leur valeur temporelle, interprétée en UTC afin que l’échelle soit indépendante du fuseau horaire du navigateur de l’utilisateur.

Les couleurs d’arrière-plan `heatmap` et `categorical` utilisent l’espace colorimétrique `oklch` en ne faisant varier que la teinte, tout en conservant une luminosité et une chroma fixes selon le thème, afin que le texte de la cellule reste lisible dans les thèmes clair et sombre. L’arrière-plan remplit toute la cellule même lorsqu’une ligne s’étend sur plus d’une ligne.

<div id="categorical-emphasis">
  ## Mise en évidence catégorielle de la sélection
</div>

En mode `categorical`, lorsqu’une cellule est sélectionnée, les autres cellules qui ont la même valeur sont mises en évidence avec une police plus grasse et une couleur de texte au contraste maximal (blanc pur dans le thème sombre, noir pur dans le thème clair). La cellule sélectionnée elle-même ne l’est pas. Il est ainsi facile de repérer où une valeur donnée apparaît ailleurs dans la colonne.

<div id="persistence">
  ## Persistance
</div>

Les modes sélectionnés sont mémorisés pour chaque colonne dans l’URL de la page et dans l’historique du navigateur, de sorte que le rechargement de la page, le partage du lien ou la navigation entre les pages précédente et suivante les conservent. Seuls les choix autres que ceux par défaut sont enregistrés, afin de garder l’URL et l’état de l’historique compacts.

<div id="limitations">
  ## Limitations
</div>

* La disposition verticale (transposée) sur une seule ligne n’affiche aucun codage couleur.
* Les différences de `DateTime64(9)` inférieures à une microseconde ne sont pas différenciées sur l’échelle de couleurs, ce qui n’est pas visuellement pertinent pour un dégradé.