---
alias: []
description: 'Documentation du format de sortie PNG'
input_format: false
keywords: ['PNG']
output_format: true
slug: /interfaces/formats/PNG
title: 'PNG'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✗      | ✔      | ✗     |

<div id="description">
  ## Description
</div>

Affiche le résultat d’une requête sous forme d’image PNG. Cela en fait un outil de visualisation intégré pratique.

La taille de l’image de sortie est fixée par les paramètres
[`output_format_image_width`](/fr/operations/settings/formats#output_format_image_width) et
[`output_format_image_height`](/fr/operations/settings/formats#output_format_image_height)
(qui valent tous deux 1024 par défaut). Les pixels non couverts par le résultat sont remplis de noir
(en modes `RGB` et en niveaux de gris) ou de noir transparent (en mode `RGBA`).

Le mode de couleur est déterminé automatiquement à partir des noms et des types de colonnes du résultat :

| Colonnes             | Mode                                                            |
| -------------------- | --------------------------------------------------------------- |
| `r`, `g`, `b`        | RGB sur 8 bits                                                  |
| `r`, `g`, `b`, `a`   | RGBA sur 8 bits                                                 |
| `v` de type entier   | niveaux de gris sur 8 bits                                      |
| `v` de type `Float*` | niveaux de gris sur 8 bits (valeurs dans `[0, 1]` → `[0, 255]`) |
| `v` de type `Bool`   | Binaire (rendu en niveaux de gris sur 8 bits : `0` ou `255`)    |

Les noms de colonnes sont comparés sans tenir compte de la casse. Si le mode de couleur ne peut pas être déterminé
sans ambiguïté (par ex. noms de colonnes inconnus, mélange de `v` avec `r`/`g`/`b`/`a`, ou absence de l’une des colonnes `r`/`g`/`b`),
la requête lève une exception.

Pour les canaux de pixels, les valeurs entières sont bornées à `[0, 255]` et les valeurs à virgule flottante
sont bornées à `[0, 1]`, puis mises à l’échelle vers `[0, 255]`.

La position de chaque enregistrement dans l’image est déterminée par l’un des deux modes suivants :

* **implicite** (par défaut — lorsque ni `x` ni `y` n’est présent). Chaque enregistrement correspond
  à un seul pixel ; les pixels sont remplis dans l’ordre de balayage : de gauche à droite, puis de haut en bas.
* **explicite** (lorsque les colonnes `x` et `y` sont présentes, toutes deux de type entier).
  Les colonnes `x` et `y` donnent les coordonnées des pixels. Les enregistrements dont les coordonnées sont en dehors
  de l’image sont ignorés silencieusement. Si plusieurs enregistrements ont les mêmes coordonnées,
  le dernier l’emporte (algorithme du peintre).

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="implicit-rgb">
  ### Coordonnées implicites (une ligne par pixel), RGB
</div>

```sql
SELECT
    toUInt8(x * 25) AS r,
    toUInt8(y * 25) AS g,
    toUInt8((x + y) * 12) AS b
FROM
(
    SELECT number % 10 AS x, intDiv(number, 10) AS y FROM numbers(100)
)
INTO OUTFILE 'gradient.png'
FORMAT PNG
SETTINGS output_format_image_width = 10, output_format_image_height = 10;
```

<div id="explicit-grayscale">
  ### Coordonnées explicites, niveaux de gris
</div>

```sql
SELECT
    toInt32(x) AS x,
    toInt32(y) AS y,
    toUInt8(intensity) AS v
FROM points
INTO OUTFILE 'points.png'
FORMAT PNG
SETTINGS output_format_image_width = 512, output_format_image_height = 512;
```

<div id="terminal-mode">
  ## Affichage des images dans le terminal
</div>

Par défaut, le format `PNG` écrit les octets bruts de l’image. Le paramètre
[`output_format_image_terminal_mode`](/fr/operations/settings/formats#output_format_image_terminal_mode)
fait plutôt afficher l’image directement dans le terminal à l’aide d’un protocole d’image intégré :

| Valeur            | Comportement                                                                                                                                            |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| &#96;&#96; (vide) | Écrit les octets bruts de l’image (comportement par défaut).                                                                                            |
| `iterm`           | Utilise le protocole d’image intégré d’iTerm2.                                                                                                          |
| `kitty`           | Utilise le protocole graphique de Kitty.                                                                                                                |
| `sixel`           | Utilise le protocole Sixel. L’image est réduite à une palette fixe de 6×6×6, et le canal alpha, le cas échéant, est composite sur un fond noir.         |
| `auto`            | Si la sortie est un terminal, détecte ses capacités et utilise `iterm`, `kitty` ou `sixel` (dans cet ordre) ; sinon, écrit les octets bruts de l’image. |

```sql
SELECT toUInt8(x * 25) AS r, toUInt8(y * 25) AS g, toUInt8((x + y) * 12) AS b
FROM (SELECT number % 10 AS x, intDiv(number, 10) AS y FROM numbers(100))
FORMAT PNG
SETTINGS output_format_image_width = 10, output_format_image_height = 10, output_format_image_terminal_mode = 'auto';
```

<div id="format-settings">
  ## Paramètres de format
</div>

| Paramètre                           | Description                                                 | Par défaut        |
| ----------------------------------- | ----------------------------------------------------------- | ----------------- |
| `output_format_image_width`         | Largeur de l&#39;image de sortie en pixels.                 | `1024`            |
| `output_format_image_height`        | Hauteur de l&#39;image de sortie en pixels.                 | `1024`            |
| `output_format_image_terminal_mode` | Protocole intégré d&#39;image de terminal (voir ci-dessus). | &#96;&#96; (vide) |