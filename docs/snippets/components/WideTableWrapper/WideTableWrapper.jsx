/**
 * WideTableWrapper — wraps a wide table and adds a synced top horizontal
 * scrollbar so readers can scroll the table without reaching the bottom.
 * Mintlify shim of clickhouse-docs's `src/components/WideTableWrapper`.
 *
 * Usage:
 *   import WideTableWrapper from "/snippets/components/WideTableWrapper/WideTableWrapper.jsx";
 *
 *   <WideTableWrapper>
 *     | col1 | col2 | … |
 *   </WideTableWrapper>
 */
const WideTableWrapper = ({ children }) => {
  const containerStyle = {
    overflow: "auto",
    maxWidth: "100%",
  };
  // Mintlify supports inline styles and renders MDX children faithfully —
  // the simple wrapper is enough for content readability. The synced
  // top-scrollbar UX in the Docusaurus original is a nice-to-have we can
  // skip without breaking content.
  return <div style={containerStyle}>{children}</div>;
};

export default WideTableWrapper;
