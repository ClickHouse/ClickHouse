const WideTableWrapper = ({ children }) => {
  const containerStyle = {
    overflow: "auto",
    maxWidth: "100%",
  };
  // Mintlify prend en charge les styles intégrés et restitue fidèlement les éléments enfants MDX —
  // ce simple wrapper suffit pour garantir la lisibilité du contenu. L’UX de la
  // barre de défilement supérieure synchronisée dans la version Docusaurus d’origine est un confort appréciable,
  // mais on peut s’en passer sans nuire au contenu.
  return <div style={containerStyle}>{children}</div>;
};
export default WideTableWrapper;