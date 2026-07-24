const {filterTokens} = require('./utility_functions.js');

/*
A custom rule that requires headings to have an explicit heading id which is unique per page.

E.g. ### Hello World {#my-explicit-id}

This is required as LLM translations break anchors by translating the heading.

The explicit ids (anchors) should be unique. E.g the following is invalid:

### First Heading {#my-explicit-id}

### Second Heading {#my-explicit-id} <--- should be unique
*/

module.exports = {
    names: ['custom-anchor-headings'],
    tags: ["headings"],
    description: 'Headings should have an explicit heading id',
    function: (params, onError) => {
        const headingIds = {};
        filterTokens(params, "heading_open", (token) => {
            if (token.markup === "#") {
                return;
            }
            const headingLine = params.lines[token.map[0]];
            const match = /\{#([a-zA-Z0-9_-]+)\}/.exec(headingLine);

            if (match) {
                const id = match[1];
                if (headingIds[id]) {
                    onError({
                        lineNumber: token.map[0] + 1,
                        detail: `Duplicate heading id: '${id}'.`,
                        context: headingLine,
                    });
                } else {
                    headingIds[id] = true;
                }
            } else {
                onError({
                    lineNumber: token.map[0] + 1, // Line number is 1-based
                    detail: `${headingLine.trim()} is missing an explicit ID (e.g., {#my-custom-id}).`,
                    context: headingLine,
                });
            };
        });
    },
};
