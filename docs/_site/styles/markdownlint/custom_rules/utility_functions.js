// Calls the provided function for each matching token.
const filterTokens = (params, type, handler) => {
    for (const token of params.parsers.markdownit.tokens) {
        if (token.type === type) {
            handler(token)
        }
    }
}

module.exports = {
    filterTokens: filterTokens,
};
