const CUICard = ({ children, className, ...props }) => (
  <div
    className={`cui-card ${className || ''}`}
    {...props}>
    {children}
  </div>
);

const Header = ({ children, className, ...props }) => (
  <div
    className={`cui-card-header ${className || ''}`}
    {...props}>
    {children}
  </div>
);

CUICard.Header = Header;

const Body = ({ children, className, ...props }) => (
  <div
    className={`cui-card-body ${className || ''}`}
    {...props}>
    {children}
  </div>
);

CUICard.Body = Body;

const Footer = ({ children, className, ...props }) => (
  <div
    className={`cui-card-footer ${className || ''}`}
    {...props}>
    {children}
  </div>
);

CUICard.Footer = Footer;

export { CUICard };