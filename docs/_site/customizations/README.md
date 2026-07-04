# Customizations

Custom scripts that customize the look and behavior of the Mintlify site.
These are loaded via the `scripts` array in `docs.json` and run on every page.

See Mintlify's docs on [custom scripts](https://www.mintlify.com/docs/customize/custom-scripts) for more information.

## Scripts

- `ask-ai-button.js` — tweaks the Ask-AI button styling/behavior.
- `cookie-banner.js` — injects and manages the cookie consent banner.
- `custom-footer.js` — renders the custom site footer (`#ch-custom-footer`).
- `kapa-init.js` — bootstraps the Kapa.ai RAG widget.
- `navbar-cta.js` — adds the "Get started" CTA to the navbar.

To wire a new script up, add a `{ "src": "/_site/customizations/<name>.js" }`
entry to the top-level `scripts` array in `docs.json`.